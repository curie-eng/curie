"""Release kind gate conditions and executable dispatch contract for #4720."""

import json
import os
import re
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/release.yaml"
KIND_JOB = "release-kind-tier"
REPOSITORY = "acme-corp/acme-bot"
TAG = "v0.0.0-test"
TAG_REF = f"refs/tags/{TAG}"
CI_RUN_ID = "7601"
RESULTS = ("success", "failure", "cancelled", "skipped")


def _workflow() -> dict[str, Any]:
    return cast(
        dict[str, Any], yaml.load(WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    )


def _needs(job: dict[str, Any]) -> list[str]:
    value = job.get("needs", [])
    return [value] if isinstance(value, str) else list(value)


def _ancestors(jobs: dict[str, Any], name: str) -> set[str]:
    ancestors: set[str] = set()
    pending = _needs(jobs[name])
    while pending:
        dependency = pending.pop()
        if dependency not in ancestors:
            ancestors.add(dependency)
            pending.extend(_needs(jobs[dependency]))
    return ancestors


def _runs(jobs: dict[str, Any], name: str, ref: str, results: dict[str, str]) -> bool:
    job = jobs[name]
    condition = " ".join(job.get("if", "true").split())
    if condition.startswith("${{") and condition.endswith("}}"):
        condition = condition[3:-2].strip()
    # GitHub applies success() implicitly unless an explicit status function is present.
    # https://docs.github.com/en/actions/learn-github-actions/expressions#status-check-functions
    statuses = [results.get(ancestor, "success") for ancestor in _ancestors(jobs, name)]
    success = all(result == "success" for result in statuses)
    if not re.search(r"\b(?:always|success|failure|cancelled)\s*\(", condition) and not success:
        return False
    condition = re.sub(
        r"needs\.([\w-]+)\.result",
        lambda match: repr(results.get(match[1], "success") if match[1] in _needs(job) else ""),
        condition,
    )
    condition = condition.replace("github.ref", repr(ref))
    condition = condition.replace("github.event_name", "'push'")
    condition = condition.replace("&&", " and ").replace("||", " or ")
    condition = re.sub(r"!(?!=)", " not ", condition)
    return bool(
        eval(
            condition.strip(),
            {"__builtins__": {}},
            {
                "true": True,
                "false": False,
                "always": lambda: True,
                "success": lambda: success,
                "failure": lambda: "failure" in statuses,
                "cancelled": lambda: "cancelled" in statuses,
                "startsWith": lambda value, prefix: value.startswith(prefix),
            },
        )
    )


def _assert_dependencies(jobs: dict[str, Any]) -> None:
    assert KIND_JOB in jobs, f"missing {KIND_JOB} job"
    for name, dependency in (
        (KIND_JOB, "authorize-release"),
        ("merge", KIND_JOB),
        ("release", KIND_JOB),
    ):
        assert dependency in _needs(jobs[name]), f"{name} must need {dependency}"
    assert "build" in _needs(jobs["merge"])
    assert {
        "merge",
        "cli-binaries",
        "chart",
        "worker-local-merge",
        "dark-factory-runner-merge",
    } <= set(_needs(jobs["release"]))
    assert KIND_JOB not in _ancestors(jobs, "build"), "build must run in parallel with kind"


def _assert_tag_publication(jobs: dict[str, Any]) -> None:
    _assert_dependencies(jobs)
    for name in ("merge", "release"):
        for result in RESULTS:
            actual = _runs(jobs, name, TAG_REF, {KIND_JOB: result})
            assert actual == (result == "success"), (
                f"{name} must require kind success on a tag: kind={result}"
            )


def test_kind_job_is_bounded_and_requires_authorization_on_a_version_tag() -> None:
    jobs = _workflow()["jobs"]
    _assert_dependencies(jobs)
    job = jobs[KIND_JOB]
    assert job["name"] == "Run the full kind tier on the tagged commit"
    assert job["permissions"] == {"actions": "write", "contents": "read"}
    assert job["timeout-minutes"] == "350"
    assert job.get("continue-on-error", "false") == "false"
    for step in job["steps"]:
        assert step.get("continue-on-error", "false") == "false"
    for result in RESULTS:
        assert _runs(jobs, KIND_JOB, TAG_REF, {"authorize-release": result}) == (
            result == "success"
        ), f"kind must require authorization success: authorize-release={result}"
    for ref in ("refs/heads/main", "refs/heads/next", "refs/tags/other"):
        assert not _runs(jobs, KIND_JOB, ref, {"authorize-release": "success"}), ref


def test_tag_publication_requires_kind_success() -> None:
    _assert_tag_publication(_workflow()["jobs"])


@pytest.mark.parametrize("branch", ["main", "next"])
def test_branch_push_skips_kind_and_preserves_continuous_manifest_publication(branch: str) -> None:
    jobs = _workflow()["jobs"]
    _assert_dependencies(jobs)
    ref = f"refs/heads/{branch}"
    results = {"authorize-release": "skipped", KIND_JOB: "skipped"}
    assert not _runs(jobs, KIND_JOB, ref, results)
    assert _runs(jobs, "build", ref, results), "branch builds must survive skipped ancestors"
    assert _runs(jobs, "merge", ref, results), "branch merge must survive skipped ancestors"
    assert not _runs(jobs, "release", ref, results)
    for result in ("failure", "cancelled", "skipped"):
        assert not _runs(jobs, "merge", ref, {**results, "build": result}), result


def test_build_does_not_wait_for_kind_but_still_requires_tag_authorization() -> None:
    jobs = _workflow()["jobs"]
    _assert_dependencies(jobs)
    for result in RESULTS:
        assert _runs(jobs, "build", TAG_REF, {KIND_JOB: result}), result
    for result in ("failure", "cancelled"):
        assert not _runs(jobs, "build", TAG_REF, {"authorize-release": result}), result


@pytest.fixture
def github(tmp_path: Path) -> dict[str, str]:
    executable = tmp_path / "gh"
    executable.write_text(
        """#!/usr/bin/env python3
import json
import os
import subprocess
import sys

args = sys.argv[1:]
assert os.environ.get("GH_TOKEN") == "test-token", "missing workflow GitHub token"
with open(os.environ["TEST_GH_CALLS"], "a", encoding="utf-8") as calls:
    calls.write(json.dumps(args) + "\\n")
if args[:1] == ["api"]:
    payload = os.environ["TEST_DISPATCH_RESPONSE"]
    if "--jq" in args:
        result = subprocess.run(
            ["jq", "-r", args[args.index("--jq") + 1]],
            input=payload, text=True, capture_output=True, check=True)
        print(result.stdout, end="")
    else:
        print(payload)
    sys.exit(0)
if args[:2] == ["run", "watch"]:
    assert os.environ.get("GH_REPO") == os.environ["GITHUB_REPOSITORY"], "wrong watch repository"
    sys.exit(int(os.environ["TEST_WATCH_EXIT"]) if "--exit-status" in args else 0)
raise SystemExit("Unexpected GitHub command: " + repr(args))
""",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    return {
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "GITHUB_REPOSITORY": REPOSITORY,
        "GITHUB_REF": TAG_REF,
        "GITHUB_REF_NAME": TAG,
        "GITHUB_EVENT_NAME": "push",
        "TEST_GH_CALLS": str(tmp_path / "calls.jsonl"),
        # https://docs.github.com/en/rest/actions/workflows#create-a-workflow-dispatch-event
        "TEST_DISPATCH_RESPONSE": json.dumps(
            {
                "workflow_run_id": int(CI_RUN_ID),
                "html_url": f"https://github.com/{REPOSITORY}/actions/runs/{CI_RUN_ID}",
            }
        ),
        # https://cli.github.com/manual/gh_run_watch documents --exit-status.
        "TEST_WATCH_EXIT": "0",
    }


def _assert_dispatch_and_watch(
    job: dict[str, Any], tmp_path: Path, github: dict[str, str], exit_code: int = 0
) -> None:
    context = {
        "github.repository": REPOSITORY,
        "github.ref": TAG_REF,
        "github.ref_name": TAG,
        "github.event_name": "push",
        "secrets.GITHUB_TOKEN": "test-token",
    }

    def render(value: str) -> str:
        return re.sub(r"\$\{\{\s*(.*?)\s*\}\}", lambda match: context[match[1]], value)

    environment = {**github, "TEST_WATCH_EXIT": str(exit_code)}
    for key, value in job.get("env", {}).items():
        environment[key] = render(value)
        context[f"env.{key}"] = environment[key]
    returncode = 0
    log = ""
    for index, step in enumerate(job["steps"]):
        if "run" not in step:
            continue
        assert "if" not in step, "dispatch and watch must run unconditionally within the kind job"
        output = tmp_path / f"step-{index}.output"
        output.write_text("", encoding="utf-8")
        result = subprocess.run(
            ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", render(step["run"])],
            cwd=tmp_path,
            env={
                **environment,
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
    assert returncode == exit_code, log
    calls_path = Path(github["TEST_GH_CALLS"])
    assert calls_path.exists(), "kind must dispatch and watch CI"
    calls = [json.loads(line) for line in calls_path.read_text(encoding="utf-8").splitlines()]
    assert len(calls) == 2, calls
    dispatch, watch = calls
    assert dispatch[0] == "api", dispatch
    assert dispatch[dispatch.index("--method") + 1] == "POST", dispatch
    assert f"repos/{REPOSITORY}/actions/workflows/ci.yaml/dispatches" in dispatch, dispatch
    fields = [
        dispatch[index + 1]
        for index, argument in enumerate(dispatch)
        if argument in ("-f", "-F", "--raw-field", "--field")
    ]
    assert sorted(fields) == sorted([f"ref={TAG}", "return_run_details=true"]), (
        "dispatch must target the tag, return its run ID, and pass no workflow inputs",
        dispatch,
    )
    typed_fields = [
        dispatch[index + 1]
        for index, argument in enumerate(dispatch)
        if argument in ("-F", "--field")
    ]
    assert "return_run_details=true" in typed_fields, "return_run_details must be a JSON boolean"
    assert not any(argument.split("=", 1)[0] == "--input" for argument in dispatch), (
        "dispatch must not supply workflow inputs"
    )
    assert watch == ["run", "watch", CI_RUN_ID, "--exit-status"], (
        "watch must wait for the dispatched run with --exit-status",
        watch,
    )


def test_dispatch_targets_the_tag_and_watches_the_returned_run(
    tmp_path: Path, github: dict[str, str]
) -> None:
    _assert_dispatch_and_watch(_workflow()["jobs"][KIND_JOB], tmp_path, github)


@pytest.mark.parametrize("exit_code", [1, 8])
def test_unsuccessful_ci_or_watch_failure_keeps_kind_red(
    tmp_path: Path, github: dict[str, str], exit_code: int
) -> None:
    _assert_dispatch_and_watch(_workflow()["jobs"][KIND_JOB], tmp_path, github, exit_code)


@pytest.mark.parametrize("name", [KIND_JOB, "merge", "release"])
def test_contract_rejects_a_removed_need(name: str) -> None:
    jobs = deepcopy(_workflow()["jobs"])
    _assert_dependencies(jobs)
    dependency = "authorize-release" if name == KIND_JOB else KIND_JOB
    jobs[name]["needs"] = [need for need in _needs(jobs[name]) if need != dependency]
    with pytest.raises(AssertionError, match=f"{name} must need {dependency}"):
        _assert_dependencies(jobs)


def test_contract_rejects_watch_without_exit_status(tmp_path: Path, github: dict[str, str]) -> None:
    job = deepcopy(_workflow()["jobs"][KIND_JOB])
    assert any("--exit-status" in step.get("run", "") for step in job["steps"])
    for step in job["steps"]:
        if "run" in step:
            step["run"] = step["run"].replace("--exit-status", "")
    with pytest.raises(AssertionError, match="watch must wait"):
        _assert_dispatch_and_watch(job, tmp_path, github)


@pytest.mark.parametrize("name", ["merge", "release"])
def test_contract_rejects_accepting_skipped_kind_on_a_tag(name: str) -> None:
    jobs = deepcopy(_workflow()["jobs"])
    _assert_tag_publication(jobs)
    condition, replacements = re.subn(
        r"needs\.release-kind-tier\.result\s*==\s*(['\"])success\1",
        lambda match: f"({match[0]} || needs.release-kind-tier.result == 'skipped')",
        jobs[name]["if"],
    )
    assert replacements == 1, "publication must explicitly require kind success"
    condition = condition.strip()
    if condition.startswith("${{") and condition.endswith("}}"):
        condition = condition[3:-2].strip()
    jobs[name]["if"] = f"always() && ({condition})"
    with pytest.raises(AssertionError, match=f"{name} must require kind success.*kind=skipped"):
        _assert_tag_publication(jobs)
