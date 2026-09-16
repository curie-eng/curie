"""Meta-tests for the release gate on the latest nightly conclusion (#2245).

A red nightly used to be an observation that did not stop a tag. These tests
pin the gate's verdicts against constructed fixtures, and pin that
`release.yaml` actually consults that verdict without a hardcoded override.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
AUTHORIZE = REPO_ROOT / "release" / "authorize.py"
NIGHTLY = REPO_ROOT / "release" / "nightly.py"
RELEASE_YAML = REPO_ROOT / ".github" / "workflows" / "release.yaml"
NIGHTLY_YAML = REPO_ROOT / ".github" / "workflows" / "nightly-graded-ladder.yaml"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


nightly = load_module(NIGHTLY, "release_nightly")
authorize_module = load_module(AUTHORIZE, "release_authorize")


class TestNightlyConclusionVerdict:
    def test_success_does_not_block(self) -> None:
        assert nightly.nightly_refusal_reason("success", allow_red=False) is None

    def test_failure_blocks_without_override(self) -> None:
        reason = nightly.nightly_refusal_reason("failure", allow_red=False)
        assert reason is not None
        assert "nightly" in reason.lower()
        assert "failure" in reason.lower()

    def test_failure_is_allowed_with_override(self) -> None:
        assert (
            nightly.nightly_refusal_reason("failure", allow_red=True) is None
        )

    def test_missing_completed_run_blocks_without_override(self) -> None:
        reason = nightly.nightly_refusal_reason(None, allow_red=False)
        assert reason is not None
        assert "no completed" in reason.lower() or "none" in reason.lower()

    def test_missing_completed_run_is_allowed_with_override(self) -> None:
        assert nightly.nightly_refusal_reason(None, allow_red=True) is None

    def test_cancelled_blocks_without_override(self) -> None:
        reason = nightly.nightly_refusal_reason("cancelled", allow_red=False)
        assert reason is not None


class TestAllowRedNightlyFromPrBody:
    def test_absent_from_bodies_is_not_an_override(self) -> None:
        assert nightly.allow_red_nightly_from_bodies(["Cut v0.9.0", ""]) is False

    def test_flag_in_a_body_is_an_override(self) -> None:
        assert nightly.allow_red_nightly_from_bodies(
            ["Release notes", "Override: --allow-red-nightly because the ladder is waived"]
        ) is True

    def test_flag_must_be_the_exact_token(self) -> None:
        assert nightly.allow_red_nightly_from_bodies(
            ["allow red nightly", "allow-red-nightly"]
        ) is False

    def test_only_merged_pr_bodies_are_consulted(self) -> None:
        bodies = nightly.merged_pr_bodies(
            [
                {"body": "--allow-red-nightly", "merged_at": None},
                {"body": "no override", "merged_at": "2026-09-01T00:00:00Z"},
            ]
        )
        assert bodies == ["no override"]


class TestAuthorizeHonorsNightly:
    def test_authorize_refuses_a_red_nightly(self, tmp_path: Path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "file.txt").write_text("x")
        import subprocess

        subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
        subprocess.run(["git", "add", "file.txt"], cwd=repo, check=True)
        subprocess.run(["git", "commit", "-m", "init"], cwd=repo, check=True)
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
        ).stdout.strip()

        with pytest.raises(authorize_module.AuthorizationError, match="nightly"):
            authorize_module.authorize(
                sha,
                [{"name": "CI", "conclusion": "success"}],
                ("main",),
                cwd=repo,
                required_names=frozenset({"CI"}),
                nightly_conclusion="failure",
                allow_red_nightly=False,
            )

    def test_authorize_accepts_a_red_nightly_with_override(self, tmp_path: Path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "file.txt").write_text("x")
        import subprocess

        subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
        subprocess.run(["git", "add", "file.txt"], cwd=repo, check=True)
        subprocess.run(["git", "commit", "-m", "init"], cwd=repo, check=True)
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
        ).stdout.strip()

        authorize_module.authorize(
            sha,
            [{"name": "CI", "conclusion": "success"}],
            ("main",),
            cwd=repo,
            required_names=frozenset({"CI"}),
            nightly_conclusion="failure",
            allow_red_nightly=True,
        )


class TestReleaseWorkflowWiresTheNightlyGate:
    def test_authorize_step_invokes_authorize_py_without_a_hardcoded_override(self) -> None:
        workflow = yaml.load(RELEASE_YAML.read_text(), Loader=yaml.BaseLoader)
        command = next(
            step["run"]
            for step in workflow["jobs"]["authorize-release"]["steps"]
            if "release/authorize.py" in step.get("run", "")
        )
        assert "release/authorize.py" in command
        assert "--allow-red-nightly" not in command

    def test_nightly_workflow_filename_is_the_one_the_gate_queries(self) -> None:
        assert nightly.NIGHTLY_WORKFLOW == "nightly-graded-ladder.yaml"
        assert NIGHTLY_YAML.is_file()
        workflow = yaml.load(NIGHTLY_YAML.read_text(), Loader=yaml.BaseLoader)
        assert workflow["name"] == "Nightly graded parity ladder"
