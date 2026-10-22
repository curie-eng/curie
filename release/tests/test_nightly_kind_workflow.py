"""Execute the nightly workflow shell with GitHub as the external boundary."""

import copy
import functools
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
CI_WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/ci.yaml"
REPOSITORY = "curie-eng/curie"
# Recorded dispatch failure texts. The first is the dispatch step log of Scheduled kind CI
# run 39402736319 on 2026-10-20: `nightly` was declared by main's ci.yaml only, so the
# dispatch to next was rejected. The second is the dispatch step log of run 35982764000
# on 2026-09-24, which failed the same way for five nights (gh printed it with a trailing
# newline). Neither reached the tracking issue, which said only to inspect the logs.
UNDECLARED_INPUT_ERROR = 'gh: Unexpected inputs provided: ["nightly"] (HTTP 422)'
NOT_A_REPOSITORY_ERROR = (
    "failed to run git: fatal: not a git repository (or any of the parent directories): .git\n"
)
NOT_A_REPOSITORY_ONE_LINE = NOT_A_REPOSITORY_ERROR.rstrip("\n")
DISPATCH_ERROR_PREFIX = "::error title=CI dispatch failed::"
DISPATCH_ERROR_LIMIT = 500
CI_RUN_ID = "7601"
CI_RUN_URL = f"https://github.com/{REPOSITORY}/actions/runs/{CI_RUN_ID}"
NIGHTLY_RUN_URL = f"https://github.com/{REPOSITORY}/actions/runs/9100"
TRACKING_TITLES = {
    "main": "Scheduled kind CI on main is failing",
    "next": "Nightly kind CI on next is failing",
}
NEXT_CRON = "30 9 * * *"
MAIN_NIGHTLY_CRON = "15 6 * * *"
MAIN_DAY_CRONS = ("15 0 * * *", "15 12 * * *", "15 18 * * *")
SCHEDULES = {"main": MAIN_NIGHTLY_CRON, "next": NEXT_CRON}


@functools.cache
def _parsed(path: Path) -> dict[str, Any]:
    """Parse a workflow once. Read-only: callers that mutate go through `_workflow`."""
    return cast(dict[str, Any], yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader))


def _workflow(path: Path = WORKFLOW) -> dict[str, Any]:
    return copy.deepcopy(_parsed(path))


class GitHubCall(TypedDict):
    args: list[str]
    repo: str | None


@dataclass
class JobResult:
    returncode: int
    outputs: dict[str, str]
    log: str


@pytest.fixture(params=["main", "next"])
def target_ref(request: pytest.FixtureRequest) -> str:
    return cast(str, request.param)


@pytest.fixture
def github(tmp_path: Path) -> dict[str, str]:
    executable = tmp_path / "gh"
    executable.write_text(
        """#!/usr/bin/env python3
import json
import os
import sys
from urllib.parse import parse_qs, urlsplit

args = sys.argv[1:]
with open(os.environ["TEST_GH_CALLS"], "a", encoding="utf-8") as calls:
    calls.write(json.dumps({"args": args, "repo": os.environ.get("GH_REPO")}) + "\\n")
def rate_limit_applies():
    scope = os.environ.get("TEST_RATE_LIMIT_ON", "any")
    if scope == "api":
        return args[:1] == ["api"]
    if scope == "issue":
        return args[:1] == ["issue"]
    return True

def consume_rate_limit():
    if not rate_limit_applies():
        return False
    path = os.environ["TEST_GH_CALLS"] + ".rate"
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(os.environ.get("TEST_RATE_LIMIT_BUDGET", "0"))
    with open(path, encoding="utf-8") as handle:
        left = int(handle.read() or "0")
    if left <= 0:
        return False
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(str(left - 1))
    return True

def emit_rate_limit():
    sys.stderr.write("HTTP 403: API rate limit exceeded for installation\\nretry-after: 0\\n")
    if "--include" in args:
        sys.stdout.write("HTTP/2 403\\nretry-after: 0\\n\\n")
        message = json.dumps({"message": "API rate limit exceeded for installation"})
        sys.stdout.write(message + "\\n")
    sys.exit(1)

def emit_included(status, body, extra_headers, exit_code=None):
    if exit_code is None:
        exit_code = 0 if status < 400 else 1
    if "--include" in args:
        sys.stdout.write(f"HTTP/2 {status}\\n")
        for header in extra_headers:
            sys.stdout.write(header + "\\n")
        sys.stdout.write("\\n")
    sys.stdout.write(body)
    if not body.endswith("\\n"):
        sys.stdout.write("\\n")
    sys.exit(exit_code)

if args[:1] == ["api"]:
    endpoint = next((arg for arg in args if arg.startswith("repos/")), "")
    url = urlsplit(endpoint)
    query = parse_qs(url.query)
    repo = os.environ["GH_REPO"]
    if url.path == f"repos/{repo}/actions/workflows/ci.yaml/dispatches":
        if os.environ.get("TEST_DISPATCH_STDERR"):
            sys.stderr.write(os.environ["TEST_DISPATCH_STDERR"])
        print(os.environ["TEST_DISPATCH_RESPONSE"])
        sys.exit(int(os.environ.get("TEST_DISPATCH_EXIT", "0")))
    if consume_rate_limit():
        emit_rate_limit()
    run_path = f"repos/{repo}/actions/runs/7601"
    if url.path == run_path:
        if os.environ.get("TEST_RUN_EXIT"):
            sys.stderr.write(os.environ.get("TEST_RUN_STDERR", "transport failure\\n"))
            sys.exit(int(os.environ["TEST_RUN_EXIT"]))
        script = os.environ.get("TEST_RUN_SCRIPT")
        if script:
            seq_path = os.environ["TEST_GH_CALLS"] + ".runseq"
            index = 0
            if os.path.exists(seq_path):
                index = int(open(seq_path, encoding="utf-8").read() or "0")
            events = json.loads(script)
            event = events[min(index, len(events) - 1)]
            with open(seq_path, "w", encoding="utf-8") as handle:
                handle.write(str(index + 1))
            emit_included(200, json.dumps(event), [])
        status = int(os.environ.get("TEST_RUN_STATUS", "200"))
        if os.environ.get("TEST_RUN_BODY"):
            emit_included(status, os.environ["TEST_RUN_BODY"], [])
        # Workflow runs: status and conclusion
        # https://docs.github.com/en/rest/actions/workflow-runs#get-a-workflow-run
        emit_included(200, json.dumps({
            "status": "completed",
            "conclusion": os.environ.get("TEST_RUN_CONCLUSION", "success"),
        }), [])
    if url.path == f"repos/{repo}/issues":
        assert query.get("state") == ["open"], query
        assert query.get("per_page") == ["100"], query
        assert set(query) <= {"state", "per_page", "page"}, query
        prefix = "TEST_ISSUE_LIST"
        pages = json.loads(os.environ["TEST_ISSUE_PAGES"])
        list_query = "state=open&per_page=100"
    elif url.path == f"repos/{repo}/actions/runs/7601/jobs":
        assert query.get("filter") == ["latest"], query
        assert query.get("per_page") == ["100"], query
        assert set(query) <= {"filter", "per_page", "page"}, query
        prefix = "TEST_JOBS"
        pages = json.loads(os.environ["TEST_JOBS_PAGES"])
        list_query = "filter=latest&per_page=100"
    else:
        raise SystemExit("Unexpected GitHub API endpoint: " + repr(endpoint))
    exit_code = int(os.environ.get(prefix + "_EXIT", "0"))
    if prefix + "_RESPONSE" in os.environ:
        emit_included(200, os.environ[prefix + "_RESPONSE"], [], exit_code)
    page_num = int((query.get("page") or ["1"])[0])
    page = pages[page_num - 1] if 0 < page_num <= len(pages) else []
    headers = []
    if page_num < len(pages):
        headers.append(
            "link: <https://api.github.com/"
            + url.path
            + "?"
            + list_query
            + "&page="
            + str(page_num + 1)
            + '>; rel="next"'
        )
    emit_included(200, json.dumps(page), headers, exit_code)
if consume_rate_limit():
    emit_rate_limit()
if args[:2] in (["issue", "create"], ["issue", "comment"], ["issue", "close"]):
    sys.exit(int(os.environ.get("TEST_" + args[1].upper() + "_EXIT",
                               os.environ.get("TEST_ISSUE_EXIT", "0"))))
if args[:2] == ["workflow", "run"]:
    print("Dispatched without observing the result")
    sys.exit(0)
raise SystemExit("Unexpected GitHub command: " + repr(args))
""",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    return {
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "TEST_GH_CALLS": str(tmp_path / "calls.jsonl"),
        # GitHub documents these response fields when return_run_details is true:
        # https://docs.github.com/en/rest/actions/workflows#create-a-workflow-dispatch-event
        "TEST_DISPATCH_RESPONSE": json.dumps(
            {"workflow_run_id": int(CI_RUN_ID), "html_url": CI_RUN_URL}
        ),
        # Repository issues include pull requests and identify authors with user.login:
        # https://docs.github.com/en/rest/issues/issues#list-repository-issues
        "TEST_ISSUE_PAGES": json.dumps([[]]),
        # Jobs pages contain a jobs array, with conclusions on jobs and individual steps:
        # https://docs.github.com/en/rest/actions/workflow-jobs#list-jobs-for-a-workflow-run
        # GitHub pagination puts the next page in a Link header:
        # https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api
        "TEST_JOBS_PAGES": json.dumps([{"total_count": 0, "jobs": []}]),
        # Tests must not sleep on the production poll or rate-limit fallback.
        "KIND_POLL_INTERVAL_SECONDS": "0",
        "KIND_RATE_LIMIT_FALLBACK_SECONDS": "0",
    }


def _run_job(
    name: str,
    tmp_path: Path,
    environment: dict[str, str],
    needs_result: str = "failure",
    needs_outputs: dict[str, str] | None = None,
    verify_result: str = "skipped",
    *,
    event_name: str = "workflow_dispatch",
    schedule: str = "",
    input_ref: str = "next",
    input_nightly: str = "true",
    workflow: dict[str, Any] | None = None,
) -> JobResult:
    job = (_parsed(WORKFLOW) if workflow is None else workflow)["jobs"][name]
    context = {
        "github.repository": REPOSITORY,
        "github.server_url": "https://github.com",
        "github.run_id": "9100",
        "github.event_name": event_name,
        "github.event.schedule": schedule,
        "inputs.ref": input_ref,
        "inputs.nightly": input_nightly,
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
    if not path.exists():
        return []
    return [
        call
        for line in path.read_text(encoding="utf-8").splitlines()
        if (call := cast(GitHubCall, json.loads(line)))["args"][: len(command)] == command
    ]


def _issue_body(environment: dict[str, str], target_ref: str) -> str:
    issues = _calls(environment, ["issue", "create"])
    assert len(issues) == 1
    arguments = issues[0]["args"]
    assert arguments[arguments.index("--repo") + 1] == REPOSITORY
    assert arguments[arguments.index("--title") + 1] == TRACKING_TITLES[target_ref]
    body = arguments[arguments.index("--body") + 1]
    assert f"{TRACKING_TITLES[target_ref].removesuffix(' is failing')} did not succeed." in body
    return body


def _assert_exact_dispatched_ci_run(
    result: JobResult,
    tmp_path: Path,
    github: dict[str, str],
    target_ref: str,
    workflow: dict[str, Any] | None = None,
    nightly: str = "true",
) -> None:
    assert result.returncode == 0, result.log
    dispatches = _calls(github, ["api"])
    assert len(dispatches) == 1
    arguments = dispatches[0]["args"]
    assert arguments[arguments.index("--method") + 1] == "POST"
    assert f"repos/{REPOSITORY}/actions/workflows/ci.yaml/dispatches" in arguments
    fields = [arguments[index + 1] for index, arg in enumerate(arguments) if arg == "-f"]
    assert fields == [f"ref={target_ref}", f"inputs[nightly]={nightly}"]
    assert arguments[arguments.index("-F") + 1] == "return_run_details=true"
    assert dispatches[0]["repo"] == REPOSITORY
    # A scheduler input that the dispatched workflow does not declare is rejected with
    # HTTP 422 before any CI runs, and the scheduler's own tests cannot see that because
    # gh is faked. Compare what this tree's scheduler sends with this tree's ci.yaml.
    sent = _dispatched_inputs(dispatches[0])
    assert sent == {"nightly": nightly}
    assert _dispatch_input_problems(_parsed(CI_WORKFLOW), sent) == []
    assert DISPATCH_ERROR_PREFIX not in result.log
    written = "".join(
        path.read_text(encoding="utf-8") for path in sorted(tmp_path.glob("dispatch.*.output"))
    )
    assert "run_id=" in written
    assert not any(line.startswith("error=") for line in written.splitlines()), written
    verify = _run_job("verify", tmp_path, github, needs_outputs=result.outputs, workflow=workflow)
    assert verify.returncode == 0, verify.log
    run_reads = [
        call
        for call in _calls(github, ["api", "--include"])
        if f"repos/{REPOSITORY}/actions/runs/{CI_RUN_ID}" in call["args"]
        and not any(arg.endswith("/jobs") or "/jobs?" in arg for arg in call["args"])
    ]
    assert [call["args"] for call in run_reads] == [
        ["api", "--include", f"repos/{REPOSITORY}/actions/runs/{CI_RUN_ID}"]
    ]
    assert not _calls(github, ["run", "watch"])
    assert result.outputs == {
        "target_ref": target_ref,
        "run_id": CI_RUN_ID,
        "run_url": CI_RUN_URL,
        "error": "",
    }
    assert not _calls(github, ["run", "list"])
    assert not _calls(github, ["issue", "create"])


# Every way the workflow picks its dispatch: each scheduled cron, then the manual path.
DISPATCH_CASES = [
    ("schedule", SCHEDULES["next"], "main", "false", "next", "true"),
    ("schedule", SCHEDULES["main"], "next", "false", "main", "true"),
    ("schedule", MAIN_DAY_CRONS[0], "next", "true", "main", "false"),
    ("schedule", MAIN_DAY_CRONS[1], "next", "true", "main", "false"),
    ("schedule", MAIN_DAY_CRONS[2], "next", "true", "main", "false"),
    ("workflow_dispatch", SCHEDULES["main"], "next", "true", "next", "true"),
    ("workflow_dispatch", SCHEDULES["next"], "main", "false", "main", "false"),
]


def _dispatched_inputs(call: GitHubCall) -> dict[str, str]:
    arguments = call["args"]
    sent: dict[str, str] = {}
    for index, arg in enumerate(arguments):
        if arg in ("-f", "-F") and (
            match := re.fullmatch(r"inputs\[([^\]]+)\]=(.*)", arguments[index + 1], re.DOTALL)
        ):
            sent[match[1]] = match[2]
    return sent


def _dispatch_input_problems(ci_workflow: dict[str, Any], sent: dict[str, str]) -> list[str]:
    """Name every sent input that the dispatched workflow would reject."""
    trigger = (ci_workflow.get("on") or {}).get("workflow_dispatch") or {}
    declared = trigger.get("inputs") or {}
    problems = []
    for name, value in sorted(sent.items()):
        if name not in declared:
            problems.append(f"{name}: not declared under workflow_dispatch.inputs")
            continue
        input_type = declared[name].get("type", "string")
        if input_type == "boolean" and value not in ("true", "false"):
            problems.append(f"{name}: {value!r} is not a boolean")
        if input_type == "choice" and value not in (declared[name].get("options") or []):
            problems.append(f"{name}: {value!r} is not one of the declared options")
    return problems


def test_dispatch_input_check_reports_an_input_ci_does_not_declare() -> None:
    ci_workflow = _workflow(CI_WORKFLOW)
    assert _dispatch_input_problems(ci_workflow, {"nightly": "true"}) == []
    del ci_workflow["on"]["workflow_dispatch"]["inputs"]["nightly"]
    problems = _dispatch_input_problems(ci_workflow, {"nightly": "true"})
    assert len(problems) == 1
    assert "nightly" in problems[0]
    assert "not declared" in problems[0]


def test_dispatch_input_check_reports_a_value_the_declared_type_rejects() -> None:
    ci_workflow = _workflow(CI_WORKFLOW)
    problems = _dispatch_input_problems(ci_workflow, {"nightly": "yes"})
    assert len(problems) == 1
    assert "nightly" in problems[0]
    ci_workflow["on"]["workflow_dispatch"]["inputs"]["nightly"] = {
        "type": "choice",
        "options": ["fast", "full"],
    }
    assert _dispatch_input_problems(ci_workflow, {"nightly": "full"}) == []
    assert len(_dispatch_input_problems(ci_workflow, {"nightly": "true"})) == 1


@pytest.mark.parametrize(
    "event_name,schedule,input_ref,input_nightly,expected_ref,expected_nightly", DISPATCH_CASES
)
def test_nightly_tracks_the_exact_dispatched_ci_run(
    tmp_path: Path,
    github: dict[str, str],
    event_name: str,
    schedule: str,
    input_ref: str,
    input_nightly: str,
    expected_ref: str,
    expected_nightly: str,
) -> None:
    result = _run_job(
        "dispatch",
        tmp_path,
        github,
        event_name=event_name,
        schedule=schedule,
        input_ref=input_ref,
        input_nightly=input_nightly,
    )
    _assert_exact_dispatched_ci_run(
        result, tmp_path, github, expected_ref, nightly=expected_nightly
    )


def test_schedule_and_manual_default_cover_both_release_trains(
    tmp_path: Path, github: dict[str, str]
) -> None:
    workflow = _workflow()
    assert workflow["name"] == "Scheduled kind CI"
    crons = [entry["cron"] for entry in workflow["on"]["schedule"]]
    assert sorted(crons) == sorted([NEXT_CRON, MAIN_NIGHTLY_CRON, *MAIN_DAY_CRONS])
    clocks = sorted(
        f"{int(cron.split()[1]):02d}:{cron.split()[0]}" for cron in crons if cron != NEXT_CRON
    )
    assert clocks == ["00:15", "06:15", "12:15", "18:15"]
    ref_input = workflow["on"]["workflow_dispatch"]["inputs"]["ref"]
    assert ref_input["type"] == "choice"
    assert ref_input["required"] == "true"
    assert ref_input["options"] == ["main", "next"]
    assert ref_input["default"] == "next"
    nightly_input = workflow["on"]["workflow_dispatch"]["inputs"]["nightly"]
    assert nightly_input["type"] == "boolean"
    assert nightly_input["default"] == "true"
    result = _run_job(
        "dispatch", tmp_path, github, input_ref=ref_input["default"], input_nightly="true"
    )
    _assert_exact_dispatched_ci_run(result, tmp_path, github, "next", nightly="true")


@pytest.mark.parametrize(
    "event_name,schedule,input_ref",
    [
        ("schedule", "0 * * * *", "next"),
        ("schedule", "", "main"),
        ("workflow_dispatch", SCHEDULES["main"], "other"),
        ("workflow_dispatch", SCHEDULES["next"], ""),
        ("push", SCHEDULES["main"], "main"),
    ],
)
def test_invalid_target_selection_fails_before_dispatch(
    tmp_path: Path,
    github: dict[str, str],
    event_name: str,
    schedule: str,
    input_ref: str,
) -> None:
    result = _run_job(
        "dispatch", tmp_path, github, event_name=event_name, schedule=schedule, input_ref=input_ref
    )
    assert result.returncode != 0, result.log
    assert not any(result.outputs.values())
    assert not _calls(github, [])


@pytest.mark.parametrize("target_output", [{}, {"target_ref": ""}, {"target_ref": "other"}])
def test_report_requires_a_valid_selected_ref_before_calling_github(
    tmp_path: Path, github: dict[str, str], target_output: dict[str, str]
) -> None:
    report = _run_job(
        "report",
        tmp_path,
        github,
        needs_result="success",
        needs_outputs={"run_id": CI_RUN_ID, "run_url": CI_RUN_URL, **target_output},
        verify_result="failure",
    )
    assert report.returncode != 0, report.log
    assert not _calls(github, [])


@pytest.mark.parametrize("mutation", ["cron", "dispatch_ref", "target_output", "tracking_title"])
def test_wrong_routing_or_tracking_title_is_detected(
    tmp_path: Path, github: dict[str, str], mutation: str
) -> None:
    workflow = _workflow()
    dispatch = workflow["jobs"]["dispatch"]
    if mutation == "tracking_title":
        report_step = workflow["jobs"]["report"]["steps"][0]
        original = '"main": "Scheduled kind CI on main"'
        assert report_step["run"].count(original) == 1
        report_step["run"] = report_step["run"].replace(
            original, '"main": "Nightly kind CI on next"'
        )
        report = _run_job(
            "report", tmp_path, github, needs_outputs={"target_ref": "main"}, workflow=workflow
        )
        assert report.returncode == 0, report.log
        with pytest.raises(AssertionError):
            _issue_body(github, "main")
        return
    if mutation == "target_output":
        dispatch["outputs"]["target_ref"] = "next"
    else:
        step_id = "target" if mutation == "cron" else "dispatch_ci"
        step = next(step for step in dispatch["steps"] if step.get("id") == step_id)
        original, replacement = (
            (f"'{SCHEDULES['main']}') target_ref=main", f"'{SCHEDULES['main']}') target_ref=next")
            if mutation == "cron"
            else ('-f "ref=$TARGET_REF"', "-f ref=next")
        )
        assert step["run"].count(original) == 1
        step["run"] = step["run"].replace(original, replacement)
    result = _run_job(
        "dispatch",
        tmp_path,
        github,
        event_name="schedule",
        schedule=SCHEDULES["main"],
        workflow=workflow,
    )
    assert result.returncode == 0, result.log
    with pytest.raises(AssertionError):
        _assert_exact_dispatched_ci_run(result, tmp_path, github, "main", workflow)


@pytest.mark.parametrize(
    "conclusion,run_exit", [("failure", ""), ("cancelled", ""), ("", "8")]
)
def test_failed_ci_keeps_nightly_red_and_files_the_exact_run(
    tmp_path: Path, github: dict[str, str], target_ref: str, conclusion: str, run_exit: str
) -> None:
    if run_exit:
        github.update(TEST_RUN_EXIT=run_exit, TEST_RUN_STDERR="HTTP 500: boom\n")
    else:
        github.update(TEST_RUN_CONCLUSION=conclusion)
    dispatch = _run_job("dispatch", tmp_path, github, input_ref=target_ref)
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
    body = _issue_body(github, target_ref)
    assert CI_RUN_URL in body
    assert NIGHTLY_RUN_URL in body
    assert verify.returncode != 0


@pytest.mark.parametrize("run_id", [None, "", "unrelated", 0])
def test_missing_or_invalid_run_id_fails_before_watching(
    tmp_path: Path, github: dict[str, str], target_ref: str, run_id: str | int | None
) -> None:
    github["TEST_DISPATCH_RESPONSE"] = json.dumps(
        {"workflow_run_id": run_id, "html_url": CI_RUN_URL}
    )
    dispatch = _run_job("dispatch", tmp_path, github, input_ref=target_ref)
    assert dispatch.returncode != 0, dispatch.log
    assert dispatch.outputs == {"target_ref": target_ref, "run_id": "", "run_url": "", "error": ""}
    assert not _calls(github, ["run", "watch"])

    report = _run_job("report", tmp_path, github, needs_outputs=dispatch.outputs)
    assert report.returncode == 0, report.log
    assert NIGHTLY_RUN_URL in _issue_body(github, target_ref)


@pytest.mark.parametrize("response", ["", "not JSON", "{}"])
def test_dispatch_failure_files_an_issue_without_guessing_a_ci_run(
    tmp_path: Path, github: dict[str, str], target_ref: str, response: str
) -> None:
    github.update(TEST_DISPATCH_EXIT="27", TEST_DISPATCH_RESPONSE=response)
    dispatch = _run_job(
        "dispatch", tmp_path, github, event_name="schedule", schedule=SCHEDULES[target_ref]
    )
    assert dispatch.returncode != 0, dispatch.log
    assert dispatch.outputs == {"target_ref": target_ref, "run_id": "", "run_url": "", "error": ""}
    arguments = _calls(github, ["api"])[0]["args"]
    assert arguments[arguments.index("-f") + 1] == f"ref={target_ref}"
    assert not _calls(github, ["run", "watch"])
    assert not _calls(github, ["run", "list"])

    report = _run_job("report", tmp_path, github, needs_outputs=dispatch.outputs)
    assert report.returncode == 0, report.log
    body = _issue_body(github, target_ref)
    assert NIGHTLY_RUN_URL in body
    assert CI_RUN_URL not in body


def _failed_dispatch(
    tmp_path: Path, github: dict[str, str], target_ref: str, stderr: str
) -> JobResult:
    github.update(TEST_DISPATCH_EXIT="1", TEST_DISPATCH_STDERR=stderr)
    return _run_job(
        "dispatch", tmp_path, github, event_name="schedule", schedule=SCHEDULES[target_ref]
    )


def _dispatch_annotations(result: JobResult) -> list[str]:
    return [line for line in result.log.splitlines() if line.startswith(DISPATCH_ERROR_PREFIX)]


# (gh stderr, the one-line error the workflow exports, whether the report adds the
# "merge main into the target" hint). Shared by the dispatch step and the report tests.
DISPATCH_ERROR_CASES = [
    pytest.param(UNDECLARED_INPUT_ERROR, UNDECLARED_INPUT_ERROR, True, id="undeclared-input"),
    pytest.param(NOT_A_REPOSITORY_ERROR, NOT_A_REPOSITORY_ONE_LINE, False, id="not-a-repository"),
    pytest.param(
        "  first line\n\n\tsecond   line \r\n",
        "first line second line",
        False,
        id="whitespace-runs",
    ),
]


@pytest.mark.parametrize("stderr,expected,_has_hint", DISPATCH_ERROR_CASES)
def test_dispatch_error_is_annotated_and_exported(
    tmp_path: Path,
    github: dict[str, str],
    target_ref: str,
    stderr: str,
    expected: str,
    _has_hint: bool,
) -> None:
    dispatch = _failed_dispatch(tmp_path, github, target_ref, stderr)
    assert dispatch.returncode != 0, dispatch.log
    assert dispatch.outputs == {
        "target_ref": target_ref,
        "run_id": "",
        "run_url": "",
        "error": expected,
    }
    annotations = _dispatch_annotations(dispatch)
    assert len(annotations) == 1, dispatch.log
    assert target_ref in annotations[0]
    assert expected in annotations[0]
    assert not _calls(github, ["run", "watch"])


@pytest.mark.parametrize("length", [DISPATCH_ERROR_LIMIT, DISPATCH_ERROR_LIMIT + 1])
def test_dispatch_error_is_capped_at_500_characters(
    tmp_path: Path, github: dict[str, str], target_ref: str, length: int
) -> None:
    expected_length = min(length, DISPATCH_ERROR_LIMIT)
    dispatch = _failed_dispatch(tmp_path, github, target_ref, "x" * length + "\n")
    assert dispatch.returncode != 0, dispatch.log
    assert dispatch.outputs["error"] == "x" * expected_length
    annotations = _dispatch_annotations(dispatch)
    assert len(annotations) == 1, dispatch.log
    assert "x" * expected_length in annotations[0]
    assert "x" * (DISPATCH_ERROR_LIMIT + 1) not in annotations[0]


def test_dispatch_error_is_a_dispatch_job_output() -> None:
    workflow = _workflow()
    assert workflow["jobs"]["dispatch"]["outputs"]["error"] == (
        "${{ steps.dispatch_ci.outputs.error }}"
    )
    report_env = workflow["jobs"]["report"]["steps"][0]["env"]
    assert report_env["DISPATCH_ERROR"] == "${{ needs.dispatch.outputs.error }}"


def test_issue_filing_failure_does_not_turn_the_nightly_green(
    tmp_path: Path, github: dict[str, str], target_ref: str
) -> None:
    github.update(TEST_RUN_CONCLUSION="failure", TEST_ISSUE_EXIT="9")
    dispatch = _run_job("dispatch", tmp_path, github, input_ref=target_ref)
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
    assert CI_RUN_URL in _issue_body(github, target_ref)


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
    tmp_path: Path,
    github: dict[str, str],
    target_ref: str,
    dispatch_result: str,
    verify_result: str,
) -> None:
    outputs = {"target_ref": target_ref}
    if dispatch_result == "success":
        dispatch = _run_job("dispatch", tmp_path, github, input_ref=target_ref)
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
    body = _issue_body(github, target_ref)
    if dispatch_result == "success":
        assert CI_RUN_URL in body
    else:
        assert CI_RUN_URL not in body
    assert f"Dispatch result: {dispatch_result}" in body
    assert f"Verification result: {verify_result}" in body


def test_report_failed_night_creates_the_tracking_issue_with_failed_jobs(
    tmp_path: Path, github: dict[str, str], target_ref: str
) -> None:
    other_ref = "next" if target_ref == "main" else "main"
    github["TEST_ISSUE_PAGES"] = json.dumps(
        [
            [
                {
                    "number": 1,
                    "title": TRACKING_TITLES[other_ref],
                    "user": {"login": "github-actions[bot]"},
                }
            ]
        ]
    )
    github["TEST_JOBS_PAGES"] = json.dumps(
        [
            {
                "total_count": 3,
                "jobs": [
                    {
                        "id": 101,
                        "name": "Python",
                        "conclusion": "failure",
                        "steps": [
                            {"name": "Checkout", "conclusion": "success"},
                            {"name": "Validate contracts", "conclusion": "failure"},
                            {"name": "Run tests", "conclusion": "cancelled"},
                        ],
                    },
                    {"id": 102, "name": "Formatting", "conclusion": "success", "steps": []},
                ],
            },
            {
                "total_count": 3,
                "jobs": [
                    {
                        "id": 103,
                        "name": "Kind",
                        "conclusion": "timed_out",
                        "steps": [{"name": "Start cluster", "conclusion": "timed_out"}],
                    }
                ],
            },
        ]
    )
    report = _run_job(
        "report",
        tmp_path,
        github,
        needs_result="success",
        needs_outputs={"target_ref": target_ref, "run_id": CI_RUN_ID, "run_url": CI_RUN_URL},
        verify_result="failure",
    )
    assert report.returncode == 0, report.log
    arguments = _calls(github, ["issue", "create"])[0]["args"]
    assert arguments[arguments.index("--title") + 1] == TRACKING_TITLES[target_ref]
    body = _issue_body(github, target_ref)
    assert f"{TRACKING_TITLES[target_ref].removesuffix(' is failing')} did not succeed." in body
    assert f"Nightly workflow: {NIGHTLY_RUN_URL}" in body
    assert "Dispatch result: success" in body
    assert "Verification result: failure" in body
    assert f"Dispatched CI run: {CI_RUN_URL}" in body
    assert "Failed jobs:" in body
    assert "1. Python: Validate contracts, Run tests" in body
    assert "2. Kind: Start cluster" in body
    assert "Formatting" not in body
    assert "Checkout" not in body
    assert not _calls(github, ["issue", "comment"])
    assert not _calls(github, ["issue", "close"])


def test_report_failed_night_comments_on_the_open_tracking_issue(
    tmp_path: Path, github: dict[str, str], target_ref: str
) -> None:
    title = TRACKING_TITLES[target_ref]
    other_ref = "next" if target_ref == "main" else "main"
    github["TEST_ISSUE_PAGES"] = json.dumps(
        [
            [
                {
                    "number": 1,
                    "title": TRACKING_TITLES[other_ref],
                    "user": {"login": "github-actions[bot]"},
                },
                {"number": 23, "title": title, "user": {"login": "github-actions[bot]"}},
            ],
            [
                {"number": 4, "title": title, "user": {"login": "github-actions[bot]"}},
                {
                    "number": 2,
                    "title": title,
                    "user": {"login": "github-actions[bot]"},
                    "pull_request": {"url": "https://api.github.com/repos/curie-eng/curie/pulls/2"},
                },
            ],
        ]
    )
    report = _run_job(
        "report",
        tmp_path,
        github,
        needs_result="success",
        needs_outputs={"target_ref": target_ref, "run_id": CI_RUN_ID, "run_url": CI_RUN_URL},
        verify_result="failure",
    )
    assert report.returncode == 0, report.log
    comments = _calls(github, ["issue", "comment"])
    assert len(comments) == 1
    arguments = comments[0]["args"]
    assert arguments[2] == "4"
    assert arguments[arguments.index("--repo") + 1] == REPOSITORY
    body = arguments[arguments.index("--body") + 1]
    assert f"{title.removesuffix(' is failing')} did not succeed." in body
    assert NIGHTLY_RUN_URL in body
    assert CI_RUN_URL in body
    assert not _calls(github, ["issue", "create"])
    assert not _calls(github, ["issue", "close"])


def test_report_ignores_same_title_issue_from_another_author(
    tmp_path: Path, github: dict[str, str], target_ref: str
) -> None:
    title = TRACKING_TITLES[target_ref]
    github["TEST_ISSUE_PAGES"] = json.dumps(
        [[{"number": 1, "title": title, "user": {"login": "acme-user"}}]]
    )
    report = _run_job("report", tmp_path, github, needs_outputs={"target_ref": target_ref})
    assert report.returncode == 0, report.log
    arguments = _calls(github, ["issue", "create"])[0]["args"]
    assert arguments[arguments.index("--title") + 1] == title
    assert NIGHTLY_RUN_URL in _issue_body(github, target_ref)
    assert not _calls(github, ["issue", "comment"])


@pytest.mark.parametrize("title_change", ["suffix", "lowercase"])
def test_report_requires_an_exact_tracking_title(
    tmp_path: Path, github: dict[str, str], target_ref: str, title_change: str
) -> None:
    title = TRACKING_TITLES[target_ref]
    title = title + " again" if title_change == "suffix" else title.lower()
    github["TEST_ISSUE_PAGES"] = json.dumps(
        [[{"number": 1, "title": title, "user": {"login": "github-actions[bot]"}}]]
    )
    report = _run_job("report", tmp_path, github, needs_outputs={"target_ref": target_ref})
    assert report.returncode == 0, report.log
    assert NIGHTLY_RUN_URL in _issue_body(github, target_ref)
    assert not _calls(github, ["issue", "comment"])


def test_report_green_night_closes_the_tracking_issue(
    tmp_path: Path, github: dict[str, str], target_ref: str
) -> None:
    other_ref = "next" if target_ref == "main" else "main"
    github["TEST_ISSUE_PAGES"] = json.dumps(
        [
            [
                {
                    "number": 1,
                    "title": TRACKING_TITLES[other_ref],
                    "user": {"login": "github-actions[bot]"},
                }
            ],
            [
                {
                    "number": 8,
                    "title": TRACKING_TITLES[target_ref],
                    "user": {"login": "github-actions[bot]"},
                }
            ],
        ]
    )
    report = _run_job(
        "report",
        tmp_path,
        github,
        needs_result="success",
        needs_outputs={"target_ref": target_ref, "run_id": CI_RUN_ID, "run_url": CI_RUN_URL},
        verify_result="success",
    )
    assert report.returncode == 0, report.log
    writes = _calls(github, ["issue"])
    assert [call["args"][:3] for call in writes] == [
        ["issue", "comment", "8"],
        ["issue", "close", "8"],
    ]
    for call in writes:
        arguments = call["args"]
        assert arguments[arguments.index("--repo") + 1] == REPOSITORY
    comment_arguments = writes[0]["args"]
    body = comment_arguments[comment_arguments.index("--body") + 1]
    assert f"{TRACKING_TITLES[target_ref].removesuffix(' is failing')} passed." in body
    assert f"Nightly workflow: {NIGHTLY_RUN_URL}" in body
    assert f"Dispatched CI run: {CI_RUN_URL}" in body
    close_arguments = writes[1]["args"]
    assert close_arguments[close_arguments.index("--reason") + 1] == "completed"
    api_calls = _calls(github, ["api"])
    assert len(api_calls) == 2
    assert all("/jobs" not in " ".join(call["args"]) for call in api_calls)


@pytest.mark.parametrize("other_issue", [False, True])
def test_report_green_night_without_issue_writes_nothing(
    tmp_path: Path, github: dict[str, str], target_ref: str, other_issue: bool
) -> None:
    if other_issue:
        other_ref = "next" if target_ref == "main" else "main"
        github["TEST_ISSUE_PAGES"] = json.dumps(
            [
                [
                    {
                        "number": 1,
                        "title": TRACKING_TITLES[other_ref],
                        "user": {"login": "github-actions[bot]"},
                    }
                ]
            ]
        )
    report = _run_job(
        "report",
        tmp_path,
        github,
        needs_result="success",
        needs_outputs={"target_ref": target_ref, "run_id": CI_RUN_ID, "run_url": CI_RUN_URL},
        verify_result="success",
    )
    assert report.returncode == 0, report.log
    assert not _calls(github, ["issue"])
    assert len(_calls(github, ["api"])) == 1


def test_report_dispatch_failure_has_no_job_list(
    tmp_path: Path, github: dict[str, str], target_ref: str
) -> None:
    report = _run_job("report", tmp_path, github, needs_outputs={"target_ref": target_ref})
    assert report.returncode == 0, report.log
    body = _issue_body(github, target_ref)
    assert NIGHTLY_RUN_URL in body
    assert "Dispatch result: failure" in body
    assert "Verification result: skipped" in body
    assert (
        "No CI run was confirmed. "
        "Inspect the nightly workflow logs for the dispatch failure." in body
    )
    assert "Failed jobs:" not in body
    assert CI_RUN_URL not in body
    assert "Dispatch error:" not in body
    assert "does not declare an input" not in body
    assert len(_calls(github, ["api"])) == 1


@pytest.mark.parametrize("stderr,expected,has_hint", DISPATCH_ERROR_CASES)
def test_report_dispatch_failure_states_the_dispatch_error_and_any_hint(
    tmp_path: Path,
    github: dict[str, str],
    target_ref: str,
    stderr: str,
    expected: str,
    has_hint: bool,
) -> None:
    dispatch = _failed_dispatch(tmp_path, github, target_ref, stderr)
    assert dispatch.returncode != 0, dispatch.log
    report = _run_job("report", tmp_path, github, needs_outputs=dispatch.outputs)
    assert report.returncode == 0, report.log
    body = _issue_body(github, target_ref)
    lines = body.splitlines()
    assert (
        "No CI run was confirmed. "
        "Inspect the nightly workflow logs for the dispatch failure." in body
    )
    assert f"Dispatch error: {expected}" in lines
    hint = (
        f"The ci.yaml on {target_ref} does not declare an input this workflow sends. "
        f"Merge main into {target_ref} so both release trains accept the same dispatch inputs."
    )
    assert (hint in lines) == has_hint
    assert ("does not declare an input" in body) == has_hint
    assert "Failed jobs:" not in body
    assert CI_RUN_URL not in body


@pytest.mark.parametrize(
    "failure", [{"TEST_JOBS_EXIT": "7"}, {"TEST_JOBS_RESPONSE": "not JSON"}]
)
def test_report_jobs_listing_failure_still_reports(
    tmp_path: Path, github: dict[str, str], target_ref: str, failure: dict[str, str]
) -> None:
    github.update(failure)
    report = _run_job(
        "report",
        tmp_path,
        github,
        needs_result="success",
        needs_outputs={"target_ref": target_ref, "run_id": CI_RUN_ID, "run_url": CI_RUN_URL},
        verify_result="failure",
    )
    assert report.returncode == 0, report.log
    body = _issue_body(github, target_ref)
    assert NIGHTLY_RUN_URL in body
    assert CI_RUN_URL in body
    assert "Failed jobs: could not be listed" in body


@pytest.mark.parametrize("conclusion", ["failure", "timed_out", "cancelled"])
def test_report_failed_job_without_failed_steps_records_its_conclusion(
    tmp_path: Path, github: dict[str, str], target_ref: str, conclusion: str
) -> None:
    github["TEST_JOBS_PAGES"] = json.dumps(
        [
            {
                "total_count": 1,
                "jobs": [
                    {
                        "id": 101,
                        "name": "Build",
                        "conclusion": conclusion,
                        "steps": [
                            {"name": "Checkout", "conclusion": "success"},
                            {"name": "Upload logs", "conclusion": "skipped"},
                        ],
                    }
                ],
            }
        ]
    )
    report = _run_job(
        "report",
        tmp_path,
        github,
        needs_result="success",
        needs_outputs={"target_ref": target_ref, "run_id": CI_RUN_ID, "run_url": CI_RUN_URL},
        verify_result="failure",
    )
    assert report.returncode == 0, report.log
    body = _issue_body(github, target_ref)
    assert f"1. Build: no failed step recorded ({conclusion})" in body
    assert "Checkout" not in body
    assert "Upload logs" not in body


@pytest.mark.parametrize("conclusion", ["success", "neutral", "skipped", "action_required", None])
def test_report_omits_jobs_without_a_failure_conclusion(
    tmp_path: Path, github: dict[str, str], target_ref: str, conclusion: str | None
) -> None:
    github["TEST_JOBS_PAGES"] = json.dumps(
        [{"total_count": 1, "jobs": [{"id": 101, "name": "Build", "conclusion": conclusion}]}]
    )
    report = _run_job(
        "report",
        tmp_path,
        github,
        needs_result="success",
        needs_outputs={"target_ref": target_ref, "run_id": CI_RUN_ID, "run_url": CI_RUN_URL},
        verify_result="failure",
    )
    assert report.returncode == 0, report.log
    body = _issue_body(github, target_ref)
    assert "Failed jobs:" in body
    assert "Build" not in body
    assert "could not be listed" not in body


@pytest.mark.parametrize(
    "failure", [{"TEST_ISSUE_LIST_EXIT": "7"}, {"TEST_ISSUE_LIST_RESPONSE": "not JSON"}]
)
def test_report_issue_listing_failure_makes_no_issue_write(
    tmp_path: Path, github: dict[str, str], target_ref: str, failure: dict[str, str]
) -> None:
    github.update(failure)
    report = _run_job("report", tmp_path, github, needs_outputs={"target_ref": target_ref})
    assert report.returncode != 0, report.log
    assert not _calls(github, ["issue"])


@pytest.mark.parametrize(
    "operation,green",
    [("create", False), ("comment", False), ("comment", True), ("close", True)],
)
def test_report_issue_write_failure_fails_the_job(
    tmp_path: Path, github: dict[str, str], target_ref: str, operation: str, green: bool
) -> None:
    github[f"TEST_{operation.upper()}_EXIT"] = "9"
    if operation != "create":
        github["TEST_ISSUE_PAGES"] = json.dumps(
            [
                [
                    {
                        "number": 8,
                        "title": TRACKING_TITLES[target_ref],
                        "user": {"login": "github-actions[bot]"},
                    }
                ]
            ]
        )
    report = _run_job(
        "report",
        tmp_path,
        github,
        needs_result="success",
        needs_outputs={"target_ref": target_ref, "run_id": CI_RUN_ID, "run_url": CI_RUN_URL},
        verify_result="success" if green else "failure",
    )
    assert report.returncode != 0, report.log
    writes = _calls(github, ["issue"])
    assert writes[-1]["args"][:2] == ["issue", operation]
    assert len(writes) == (2 if operation == "close" else 1)


def test_reporting_is_independent_bounded_and_cannot_swallow_dispatch_failure() -> None:
    workflow = _workflow()
    dispatch = workflow["jobs"]["dispatch"]
    verify = workflow["jobs"]["verify"]
    report = workflow["jobs"]["report"]
    assert verify["needs"] == "dispatch"
    assert set(report["needs"]) == {"dispatch", "verify"}
    assert report["if"] == "${{ always() }}"
    assert report["name"] == "Report scheduled kind CI result"
    assert (workflow.get("permissions") or {}).get("issues") != "write"
    assert report["permissions"] == {"issues": "write", "actions": "read"}
    assert report["timeout-minutes"] == "75"
    assert report["steps"][0]["run"].startswith("python3 - <<'PY'\n")
    assert dispatch["permissions"].get("issues") != "write"
    assert verify["permissions"].get("issues") != "write"
    assert sum(int(job["timeout-minutes"]) for job in (dispatch, verify, report)) < 24 * 60
    for job in (dispatch, verify, report):
        assert 0 < int(job["timeout-minutes"]) < 24 * 60
        assert job.get("continue-on-error", "false") == "false"
        assert all(step.get("continue-on-error", "false") == "false" for step in job["steps"])
        assert all("uses" not in step for step in job["steps"])


def _run_reads(github: dict[str, str]) -> list[GitHubCall]:
    return [
        call
        for call in _calls(github, ["api", "--include"])
        if f"/actions/runs/{CI_RUN_ID}" in " ".join(call["args"])
        and "/jobs" not in " ".join(call["args"])
    ]


def test_verify_waits_out_a_rate_limit_then_accepts_success(
    tmp_path: Path, github: dict[str, str]
) -> None:
    github.update(TEST_RATE_LIMIT_BUDGET="2", TEST_RATE_LIMIT_ON="api")
    verify = _run_job(
        "verify",
        tmp_path,
        github,
        needs_outputs={"run_id": CI_RUN_ID, "run_url": CI_RUN_URL, "target_ref": "main"},
    )
    assert verify.returncode == 0, verify.log
    assert len(_run_reads(github)) == 3


def test_verify_does_not_retry_a_non_rate_limit_denial(
    tmp_path: Path, github: dict[str, str]
) -> None:
    github.update(
        TEST_RUN_STATUS="403",
        TEST_RUN_BODY=json.dumps({"message": "Resource not accessible by integration"}),
    )
    verify = _run_job(
        "verify",
        tmp_path,
        github,
        needs_outputs={"run_id": CI_RUN_ID, "run_url": CI_RUN_URL, "target_ref": "main"},
    )
    assert verify.returncode != 0, verify.log
    assert len(_run_reads(github)) == 1


def test_verify_polls_a_running_dispatch_until_it_completes(
    tmp_path: Path, github: dict[str, str]
) -> None:
    github["TEST_RUN_SCRIPT"] = json.dumps(
        [
            {"status": "in_progress", "conclusion": None},
            {"status": "completed", "conclusion": "success"},
        ]
    )
    verify = _run_job(
        "verify",
        tmp_path,
        github,
        needs_outputs={"run_id": CI_RUN_ID, "run_url": CI_RUN_URL, "target_ref": "main"},
    )
    assert verify.returncode == 0, verify.log
    assert len(_run_reads(github)) == 2


def test_verify_poll_interval_defaults_to_two_minutes() -> None:
    step = _workflow()["jobs"]["verify"]["steps"][0]
    assert 'KIND_POLL_INTERVAL_SECONDS", "120"' in step["run"]
    assert "run watch" not in step["run"]
    assert "retry-after" in step["run"]


def test_report_files_the_issue_after_a_brief_rate_limit(
    tmp_path: Path, github: dict[str, str], target_ref: str
) -> None:
    github.update(TEST_RATE_LIMIT_BUDGET="1", TEST_RATE_LIMIT_ON="api")
    report = _run_job(
        "report",
        tmp_path,
        github,
        needs_result="success",
        needs_outputs={"target_ref": target_ref, "run_id": CI_RUN_ID, "run_url": CI_RUN_URL},
        verify_result="failure",
    )
    assert report.returncode == 0, report.log
    assert CI_RUN_URL in _issue_body(github, target_ref)
    assert len(_calls(github, ["issue", "create"])) == 1


def test_report_retries_issue_create_after_a_brief_rate_limit(
    tmp_path: Path, github: dict[str, str], target_ref: str
) -> None:
    github.update(TEST_RATE_LIMIT_BUDGET="1", TEST_RATE_LIMIT_ON="issue")
    report = _run_job("report", tmp_path, github, needs_outputs={"target_ref": target_ref})
    assert report.returncode == 0, report.log
    assert len(_calls(github, ["issue", "create"])) == 2


def test_report_closes_the_issue_after_a_brief_rate_limit(
    tmp_path: Path, github: dict[str, str], target_ref: str
) -> None:
    github.update(TEST_RATE_LIMIT_BUDGET="1", TEST_RATE_LIMIT_ON="issue")
    github["TEST_ISSUE_PAGES"] = json.dumps(
        [
            [
                {
                    "number": 8,
                    "title": TRACKING_TITLES[target_ref],
                    "user": {"login": "github-actions[bot]"},
                }
            ]
        ]
    )
    report = _run_job(
        "report",
        tmp_path,
        github,
        needs_result="success",
        needs_outputs={"target_ref": target_ref, "run_id": CI_RUN_ID, "run_url": CI_RUN_URL},
        verify_result="success",
    )
    assert report.returncode == 0, report.log
    writes = _calls(github, ["issue"])
    assert [call["args"][:2] for call in writes] == [
        ["issue", "comment"],
        ["issue", "comment"],
        ["issue", "close"],
    ]
