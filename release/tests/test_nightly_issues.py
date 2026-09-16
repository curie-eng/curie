"""Tests for nightly-ladder issue filing and signature dedup (#2245)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
NIGHTLY = REPO_ROOT / "release" / "nightly.py"
NIGHTLY_YAML = REPO_ROOT / ".github" / "workflows" / "nightly-graded-ladder.yaml"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


nightly = load_module(NIGHTLY, "release_nightly")


DISPATCHER_LOG = (
    "\n########## rung: local-release (compose, generated release artifact) "
    "##########\n"
    "error: image 'ghcr.io/curie-eng/curie-dispatcher:latest' is required by "
    "compose.release.yaml's full profile and is not present locally.\n"
    "fix: build and tag the missing image(s) locally\n"
)

CLUSTER_LOG = (
    "\n=== curie cluster message after repeated eval ===\n"
    "cluster: message after repeated eval timed out at 45s without a "
    "finalized reply; eval-owned sandboxes likely still hold the quota "
    "(#1534).\n"
)

SKILL_UP_LOG = "\n=== curie skill up (fake model, offline) ===\n"

OTEL_LOG = (
    "\n=== case: local runner failure is observable and recovers ===\n"
    "AssertionError: no new worker-to-runner trace carried ERROR on "
    "turn.process + agent.run and classified_failure\n"
)


class TestExtractSignatures:
    def test_dispatcher_image_signature_is_stable(self) -> None:
        signatures = nightly.extract_signatures(
            [{"name": "local-release", "conclusion": "failure", "log": DISPATCHER_LOG}]
        )
        assert len(signatures) == 1
        assert "curie-dispatcher:latest" in signatures[0].text
        assert signatures[0].job == "local-release"

    def test_cluster_timeout_signature_is_stable(self) -> None:
        signatures = nightly.extract_signatures(
            [{"name": "cluster", "conclusion": "failure", "log": CLUSTER_LOG}]
        )
        assert len(signatures) == 1
        assert "timed out at 45s" in signatures[0].text

    def test_skill_up_banner_without_diagnostic_is_a_signature(self) -> None:
        signatures = nightly.extract_signatures(
            [{"name": "skill+local connector", "conclusion": "failure", "log": SKILL_UP_LOG}]
        )
        assert len(signatures) == 1
        assert "skill up" in signatures[0].text.lower()

    def test_skill_up_banner_with_a_later_error_uses_the_error(self) -> None:
        log = SKILL_UP_LOG + "error: connector image missing\n"
        signatures = nightly.extract_signatures(
            [{"name": "skill+local connector", "conclusion": "failure", "log": log}]
        )
        assert len(signatures) == 1
        assert "connector image missing" in signatures[0].text
        assert "skill up" not in signatures[0].text.lower()

    def test_otel_assertion_signature_is_stable(self) -> None:
        signatures = nightly.extract_signatures(
            [{"name": "skill+local default", "conclusion": "failure", "log": OTEL_LOG}]
        )
        assert len(signatures) == 1
        assert "classified_failure" in signatures[0].text

    def test_successful_jobs_emit_no_signatures(self) -> None:
        assert nightly.extract_signatures(
            [{"name": "build-cli", "conclusion": "success", "log": "ok"}]
        ) == []

    def test_same_failure_twice_dedups_to_one_signature_id(self) -> None:
        first = nightly.extract_signatures(
            [{"name": "a", "conclusion": "failure", "log": DISPATCHER_LOG}]
        )[0]
        second = nightly.extract_signatures(
            [{"name": "b", "conclusion": "failure", "log": DISPATCHER_LOG}]
        )[0]
        assert first.signature_id == second.signature_id


class TestPlanIssueActions:
    def test_unknown_signature_is_created(self) -> None:
        signatures = nightly.extract_signatures(
            [{"name": "local-release", "conclusion": "failure", "log": DISPATCHER_LOG}]
        )
        actions = nightly.plan_issue_actions(signatures, existing_issues=[])
        assert len(actions) == 1
        assert actions[0].kind == "create"
        assert nightly.NIGHTLY_LABEL in actions[0].labels
        assert nightly.signature_marker(signatures[0].signature_id) in actions[0].body

    def test_known_signature_is_commented_not_recreated(self) -> None:
        signatures = nightly.extract_signatures(
            [{"name": "local-release", "conclusion": "failure", "log": DISPATCHER_LOG}]
        )
        marker = nightly.signature_marker(signatures[0].signature_id)
        existing = [
            {
                "number": 99,
                "title": "nightly-ladder: dispatcher image",
                "body": f"prior run\n{marker}\n",
            }
        ]
        actions = nightly.plan_issue_actions(signatures, existing_issues=existing)
        assert len(actions) == 1
        assert actions[0].kind == "comment"
        assert actions[0].number == 99

    def test_unrelated_open_issue_does_not_absorb_a_new_signature(self) -> None:
        signatures = nightly.extract_signatures(
            [{"name": "cluster", "conclusion": "failure", "log": CLUSTER_LOG}]
        )
        existing = [
            {
                "number": 1,
                "title": "other",
                "body": nightly.signature_marker("deadbeefdeadbeef"),
            }
        ]
        actions = nightly.plan_issue_actions(signatures, existing_issues=existing)
        assert actions[0].kind == "create"


class TestNightlyWorkflowFilesIssues:
    def test_file_failures_job_is_gated_on_a_failed_ladder_job(self) -> None:
        workflow = yaml.load(NIGHTLY_YAML.read_text(), Loader=yaml.BaseLoader)
        job = workflow["jobs"]["file-failures"]
        needs = job["needs"]
        needs_list = [needs] if isinstance(needs, str) else list(needs)
        for name in (
            "ladder-skill-local",
            "ladder-local-release",
            "ladder-cluster",
        ):
            assert name in needs_list
        condition = " ".join(job["if"].split())
        assert "always()" in condition
        assert "failure" in condition
        assert "success" not in condition or "needs." in condition

    def test_file_failures_job_has_issues_write_and_does_not_raise_workflow_contents(self) -> None:
        source = NIGHTLY_YAML.read_text()
        workflow = yaml.load(source, Loader=yaml.BaseLoader)
        assert workflow["permissions"]["contents"] == "read"
        job = workflow["jobs"]["file-failures"]
        assert job["permissions"]["issues"] == "write"
        assert (job["permissions"].get("contents") or "read") == "read"

    def test_file_failures_invokes_the_filer_with_the_run_id(self) -> None:
        workflow = yaml.load(NIGHTLY_YAML.read_text(), Loader=yaml.BaseLoader)
        runs = [
            step.get("run", "")
            for step in workflow["jobs"]["file-failures"]["steps"]
        ]
        joined = "\n".join(runs)
        assert "release/nightly.py" in joined
        assert "GITHUB_RUN_ID" in joined
        assert "file-issues" in joined or "--run-id" in joined
