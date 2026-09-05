"""Execute the workflow detector with real git changes and its output consumer."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
DETECTOR = ROOT / "tools/sdk-lock-gate/detect.py"
WORKFLOW = ROOT / ".github/workflows/sdk-approval-gate.yaml"
LOCK = 'version = 1\n[[package]]\nname = "claude-agent-sdk"\nversion = "0.2.135"\n'

# Deliberately independent of the detector: removing one production path must red.
POLICY_PATHS = (
    "runner/src/curie_runner/approval.py",
    "runner/src/curie_runner/adapter.py",
    "runner/src/curie_runner/__main__.py",
    "runner/src/curie_runner/connectors.py",
    "runner/src/curie_runner/hooks.py",
    "runner/src/curie_runner/config.py",
    "runner/src/curie_runner/session.py",
    "runner/tests/test_approval.py",
    "runner/tests/test_approval_gate_enforcement.py",
    "runner/tests/test_gate_shadowing.py",
    "runner/tests/test_gate_e2e.py",
    "runner/tests/test_hooks.py",
    "runner/tests/test_connectors.py",
    "runner/tests/test_mcp_tool_capability.py",
    "runner/tests/test_hosted_mcp_approval_catalog.py",
    "runner/tests/test_live.py",
    "runner/tests/test_ladder_approval_gate_case.py",
    "runner/tests/fixtures/mcp_tool_capability_server.py",
)


def detect(tmp_path: Path, paths: bytes, *, new_lock: str = LOCK) -> tuple[int, str, str]:
    (tmp_path / "old.lock").write_text(LOCK)
    (tmp_path / "uv.lock").write_text(new_lock)
    (tmp_path / "paths").write_bytes(paths)
    output = tmp_path / "output"
    result = subprocess.run(
        [
            sys.executable,
            str(DETECTOR),
            "--old-file",
            str(tmp_path / "old.lock"),
            "--new-file",
            str(tmp_path / "uv.lock"),
            "--changed-paths-file",
            str(tmp_path / "paths"),
        ],
        env=dict(os.environ, GITHUB_OUTPUT=str(output)),
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode, output.read_text() if output.exists() else "", result.stderr


@pytest.mark.parametrize("path", POLICY_PATHS)
def test_each_enforcement_path_runs_live_with_unchanged_lock(tmp_path: Path, path: str) -> None:
    code, output, error = detect(tmp_path, path.encode() + b"\0")
    assert code == 0, error
    assert output == "changed=true\n"
    workflow = yaml.safe_load(WORKFLOW.read_text())
    assert path in workflow[True]["pull_request"]["paths"]
    assert (ROOT / path).is_file()


@pytest.mark.parametrize(
    "path",
    [
        "README.md",
        "tools/sdk-lock-gate/README.md",
        "tools/sdk-lock-gate/tests/README.md",
        "tools/sdk-lock-gate/detect.py.bak",
        "runner/src/curie_runner/otel.py",
        "runner/tests/test_otel.py",
        "runner/src/curie_runner/approval.py.bak",
        "apps/ui/src/App.tsx",
    ],
)
def test_unrelated_paths_do_not_run_live(tmp_path: Path, path: str) -> None:
    assert detect(tmp_path, path.encode() + b"\0")[:2] == (0, "changed=false\n")


@pytest.mark.parametrize(
    "path",
    [
        ".github/workflows/sdk-approval-gate.yaml",
        "tools/sdk-lock-gate/detect.py",
        "tools/sdk-lock-gate/tests/test_approval_paths.py",
        "tools/sdk-lock-gate/run-proof.py",
        "tools/sdk-lock-gate/tests/test_live_proof_consumer.py",
        "tools/sdk-lock-gate/tests/test_sdk_lock_gate.py",
    ],
)
def test_gate_changes_reprove_the_guard(tmp_path: Path, path: str) -> None:
    assert detect(tmp_path, path.encode() + b"\0")[:2] == (0, "changed=true\n")


@pytest.mark.parametrize("paths", [b"unterminated", b"\xff\0", b"\0", b"../outside\0"])
def test_malformed_path_input_cannot_false_green_skip(tmp_path: Path, paths: bytes) -> None:
    code, output, _ = detect(tmp_path, paths)
    assert code != 0 or output == "changed=true\n"
    assert output != "changed=false\n"


def test_lock_semantics_remain_in_the_workflow_consumer(tmp_path: Path) -> None:
    assert detect(tmp_path, b"uv.lock\0", new_lock=LOCK.replace("0.2.135", "0.2.140"))[:2] == (
        0,
        "changed=true\n",
    )
    (tmp_path / "output").unlink()
    assert detect(tmp_path, b"uv.lock\0", new_lock=LOCK + "\n# other package changed\n")[:2] == (
        0,
        "changed=false\n",
    )


def test_deleted_enforcement_file_is_in_real_git_path_stream(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args: str) -> bytes:
        return subprocess.check_output(["git", "-C", str(repo), *args])

    git("init", "-q")
    file = repo / POLICY_PATHS[0]
    file.parent.mkdir(parents=True)
    file.write_text("old policy\n")
    git("add", ".")
    git("-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-qm", "base")
    file.unlink()
    paths = git("diff", "--name-only", "--no-renames", "-z", "HEAD")
    assert detect(tmp_path, paths)[:2] == (0, "changed=true\n")


def test_workflow_materializes_immutable_pr_range_and_uses_proof_consumer() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    detect_job = workflow["jobs"]["detect"]
    runs = "\n".join(step.get("run", "") for step in detect_job["steps"])
    assert "--no-renames -z" in runs
    assert "github.event.pull_request.base.sha" in str(detect_job)
    assert "github.event.pull_request.head.sha" in str(detect_job)
    assert "--changed-paths-file" in runs
    live = workflow["jobs"]["live-approval-gate"]
    assert live["if"] == "needs.detect.outputs.changed == 'true'"
    proof = next(step for step in live["steps"] if "run-proof.py" in step.get("run", ""))
    assert proof["env"]["CURIE_E2E_LIVE"] == "1"
    assert proof["env"]["CURIE_E2E_TIERS"] == "skill"


def test_workflow_outer_filter_is_only_named_policy_and_gate_paths() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    assert set(workflow[True]["pull_request"]["paths"]) == set(POLICY_PATHS) | {
        "uv.lock",
        ".github/workflows/sdk-approval-gate.yaml",
        "tools/sdk-lock-gate/detect.py",
        "tools/sdk-lock-gate/run-proof.py",
        "tools/sdk-lock-gate/tests/test_approval_paths.py",
        "tools/sdk-lock-gate/tests/test_live_proof_consumer.py",
        "tools/sdk-lock-gate/tests/test_sdk_lock_gate.py",
    }


def test_workflow_materialization_executes_against_exact_git_commits(tmp_path: Path) -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    step = next(
        step
        for step in workflow["jobs"]["detect"]["steps"]
        if step.get("name") == "Materialize the exact PR change and lock inputs"
    )
    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args: str) -> str:
        return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()

    git("init", "-q")
    file = repo / POLICY_PATHS[0]
    file.parent.mkdir(parents=True)
    file.write_text("old policy\n")
    (repo / "uv.lock").write_text(LOCK)
    git("add", ".")
    git("-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-qm", "base")
    base = git("rev-parse", "HEAD")
    file.unlink()
    git("add", ".")
    git("-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-qm", "head")
    head = git("rev-parse", "HEAD")
    # A dirty working lock must not substitute for the PR head's lock.
    (repo / "uv.lock").write_text("broken working file")
    result = subprocess.run(
        ["bash", "-c", step["run"]],
        cwd=repo,
        env=dict(os.environ, BASE_SHA=base, HEAD_SHA=head),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert (repo / "old.lock").read_text() == LOCK
    assert (repo / "new.lock").read_text() == LOCK
    assert detect(tmp_path, (repo / "changed-paths").read_bytes())[:2] == (0, "changed=true\n")
    result = subprocess.run(
        ["bash", "-c", step["run"]],
        cwd=repo,
        env=dict(os.environ, BASE_SHA="missing-ref", HEAD_SHA=head),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
