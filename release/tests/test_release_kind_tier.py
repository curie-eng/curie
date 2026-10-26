"""Release kind gate conditions and executable reuse contract for #4863 and #4720."""

import json
import re
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from release.tests.test_kind_ci import (
    COMMIT,
    HISTORY,
    PREFIX,
    REPOSITORY,
    ROOT,
    RUN_ID,
    RUN_URL,
    STATUS,
    TAG,
    GitHub,
    assert_dispatch,
)

WORKFLOW = ROOT / ".github/workflows/release.yaml"
KIND_JOB = "release-kind-tier"
TAG_REF = f"refs/tags/{TAG}"
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
def github(tmp_path: Path) -> GitHub:
    return GitHub(tmp_path)


def _assert_kind_command(
    job: dict[str, Any], github: GitHub, *, reused: bool = False, success: bool = True
) -> None:
    context = {
        "github.repository": REPOSITORY,
        "github.ref": TAG_REF,
        "github.ref_name": TAG,
        "github.event_name": "push",
        "github.sha": COMMIT,
        "secrets.GITHUB_TOKEN": "test-token",
    }

    def render(value: str) -> str:
        return re.sub(r"\$\{\{\s*(.*?)\s*\}\}", lambda match: context[match[1]], value)

    environment = {
        **github.env,
        "GITHUB_REPOSITORY": REPOSITORY,
        "GITHUB_REF": TAG_REF,
        "GITHUB_REF_NAME": TAG,
        "GITHUB_EVENT_NAME": "push",
    }
    for key, value in job.get("env", {}).items():
        environment[key] = render(value)
        context[f"env.{key}"] = environment[key]
    returncode = 0
    log = ""
    for step in job["steps"]:
        if "run" not in step:
            continue
        assert "if" not in step, "the kind proof command must run unconditionally"
        result = subprocess.run(
            ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", render(step["run"])],
            cwd=ROOT,
            env={
                **environment,
                **{key: render(value) for key, value in step.get("env", {}).items()},
            },
            capture_output=True,
            text=True,
            timeout=10,
        )
        log += result.stdout + result.stderr
        returncode = result.returncode
        if returncode:
            break
    assert (returncode == 0) == success, f"child result must determine the gate: {log}"
    commit_reads = [path for path, _ in github.calls("GET") if "/commits/" in path]
    assert commit_reads and commit_reads[0] == f"{PREFIX}/commits/{COMMIT}", log
    if reused:
        assert not github.calls("POST"), "complete recorded proof must avoid dispatch"
    else:
        assert_dispatch(github)
    if success:
        payload = json.loads(result.stdout)
        assert payload == {
            "mode": "reused" if reused else "dispatched",
            "run_id": HISTORY["run"]["id"] if reused else RUN_ID,
            "run_url": HISTORY["run"]["html_url"] if reused else RUN_URL,
        }
        assert payload["run_url"] in log


def test_workflow_reuses_recorded_same_tree_kind_proof(github: GitHub) -> None:
    _assert_kind_command(_workflow()["jobs"][KIND_JOB], github, reused=True)


def test_workflow_dispatch_targets_the_tag_and_waits_for_returned_run(github: GitHub) -> None:
    github.history([])
    _assert_kind_command(_workflow()["jobs"][KIND_JOB], github)


@pytest.mark.parametrize("conclusion", ["failure", "cancelled"])
def test_unsuccessful_child_keeps_workflow_kind_gate_red(github: GitHub, conclusion: str) -> None:
    github.history([])
    github.reply(STATUS, {"status": "completed", "conclusion": conclusion})
    _assert_kind_command(_workflow()["jobs"][KIND_JOB], github, success=False)


@pytest.mark.parametrize("name", [KIND_JOB, "merge", "release"])
def test_contract_rejects_a_removed_need(name: str) -> None:
    jobs = deepcopy(_workflow()["jobs"])
    _assert_dependencies(jobs)
    dependency = "authorize-release" if name == KIND_JOB else KIND_JOB
    jobs[name]["needs"] = [need for need in _needs(jobs[name]) if need != dependency]
    with pytest.raises(AssertionError, match=f"{name} must need {dependency}"):
        _assert_dependencies(jobs)


def test_contract_rejects_ignoring_failed_child(github: GitHub) -> None:
    github.history([])
    github.reply(STATUS, {"status": "completed", "conclusion": "failure"})
    job = deepcopy(_workflow()["jobs"][KIND_JOB])
    for step in job["steps"]:
        if "run" in step:
            step["run"] = step["run"].rstrip() + " || true\n"
    with pytest.raises(AssertionError, match="child result must determine the gate"):
        _assert_kind_command(job, github, success=False)


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
