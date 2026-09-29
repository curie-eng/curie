"""Negative and positive controls for the runtime assertion gate (#3391)."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
GATE_DIR = REPO_ROOT / "tools" / "runtime-assertion-gate"
RUNNER = GATE_DIR / "run.sh"
CI = REPO_ROOT / ".github" / "workflows" / "ci.yaml"
SCRIPT = "charts/curie/ci/runtime/example-runtime.sh"

_spec = importlib.util.spec_from_file_location("runtime_assertion_gate", GATE_DIR / "gate.py")
assert _spec and _spec.loader
gate = importlib.util.module_from_spec(_spec)
sys.modules["runtime_assertion_gate"] = gate
_spec.loader.exec_module(gate)

CALICO = (
    "kubectl apply -f "
    "https://raw.githubusercontent.com/projectcalico/calico/v3.28.2/manifests/calico.yaml\n"
    "kubectl -n kube-system rollout status daemonset/calico-node --timeout=300s"
)
INVOKE = f"bash tools/runtime-assertion-gate/run.sh {SCRIPT}"


def _workflow(tmp_path: Path, **overrides: object) -> Path:
    steps: list[dict[str, object]] = [
        {"uses": "actions/checkout@v7"},
        {"name": "Install Calico", "run": CALICO},
        {"name": "Run it", "run": INVOKE},
    ]
    job: dict[str, object] = {
        "if": "${{ needs.changes.outputs.cluster == 'true' }}",
        "needs": ["changes"],
        "steps": steps,
    }
    step_overrides = overrides.pop("step", None)
    if isinstance(step_overrides, dict):
        steps[2].update(step_overrides)
    if overrides.pop("no_calico", False):
        del steps[1]
    if overrides.pop("calico_after", False):
        steps.append(steps.pop(1))
    job.update(overrides)
    document = {
        "jobs": {
            "changes": {"steps": []},
            "cluster": job,
            "e2e-required": {"needs": ["changes", "cluster"], "steps": []},
        }
    }
    path = tmp_path / "ci.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return path


def test_registered_assertion_passes(tmp_path: Path) -> None:
    gate.check_registration(_workflow(tmp_path), [SCRIPT])


def test_unregistered_assertion_fails_with_path_and_missing_step(tmp_path: Path) -> None:
    missing = "charts/curie/ci/runtime/connector-readiness-runtime.sh"
    with pytest.raises(gate.GateError) as error:
        gate.check_registration(_workflow(tmp_path), [missing])
    message = str(error.value)
    assert missing in message
    assert "unregistered" in message
    assert f"bash tools/runtime-assertion-gate/run.sh {missing}" in message


@pytest.mark.parametrize(
    ("overrides", "problem"),
    [
        ({"no_calico": True}, "does not install Calico"),
        ({"calico_after": True}, "does not install Calico"),
        ({"if": "${{ always() }}"}, "is not selected by"),
        ({"if": "${{ needs.changes.outputs.cluster == 'true' && false }}"}, "is not selected by"),
        (
            {"step": {"run": f"if false; then\n  {INVOKE}\nfi"}},
            "runs other commands",
        ),
        ({"step": {"run": f"set -euo pipefail\n{INVOKE} --flag \\\n  value"}}, None),
        ({"step": {"if": "${{ false }}"}}, "can be skipped"),
        ({"step": {"continue-on-error": True}}, "continue-on-error"),
        ({"continue-on-error": True}, "continue-on-error"),
        ({"step": {"run": f"bash {SCRIPT}"}}, "unregistered"),
        ({"step": {"run": f"# bash tools/runtime-assertion-gate/run.sh {SCRIPT}"}}, "unregistered"),
    ],
)
def test_disqualified_invocation_fails(
    tmp_path: Path, overrides: dict[str, object], problem: str | None
) -> None:
    workflow = _workflow(tmp_path, **overrides)
    if problem is None:
        # Positive control: set options and continuation lines still register.
        gate.check_registration(workflow, [SCRIPT])
        return
    with pytest.raises(gate.GateError, match=problem):
        gate.check_registration(workflow, [SCRIPT])


def test_conditional_calico_install_does_not_count(tmp_path: Path) -> None:
    path = _workflow(tmp_path)
    document = yaml.safe_load(path.read_text())
    document["jobs"]["cluster"]["steps"][1]["if"] = "${{ false }}"
    path.write_text(yaml.safe_dump(document))
    with pytest.raises(gate.GateError, match="does not install Calico"):
        gate.check_registration(path, [SCRIPT])


def test_job_outside_the_required_verdict_fails(tmp_path: Path) -> None:
    path = _workflow(tmp_path)
    document = yaml.safe_load(path.read_text())
    document["jobs"]["e2e-required"]["needs"] = ["changes"]
    path.write_text(yaml.safe_dump(document))
    with pytest.raises(gate.GateError, match="not a dependency of e2e-required"):
        gate.check_registration(path, [SCRIPT])


def test_real_ci_registers_its_calico_runtime_assertions() -> None:
    gate.check_registration(
        CI,
        [
            "charts/curie/ci/runtime/publication-job-assertions.sh",
            "charts/curie/ci/runtime/langfuse-postgres-readiness-runtime.sh",
            "charts/curie/ci/runtime/connector-readiness-runtime.sh",
            "charts/curie/ci/runtime/e2e-connector-identity-runtime.sh",
            "charts/curie/ci/runtime/runner-resources-assertions.sh",
        ],
    )


def test_real_ci_does_not_register_an_assertion_the_parity_ladder_only_covers() -> None:
    # The general cluster ladder runs on every chart change; it is not proof.
    with pytest.raises(gate.GateError, match="unregistered"):
        gate.check_registration(CI, ["charts/curie/ci/runtime/metrics-alerts-runtime.sh"])


def _repo(tmp_path: Path, body: str) -> Path:
    repo = tmp_path / "repo"
    (repo / "charts/curie/ci/runtime").mkdir(parents=True)
    (repo / SCRIPT).write_text(body, encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    return repo


def _run(repo: Path, receipts: Path, script: str = SCRIPT) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "RUNTIME_ASSERTION_RECEIPTS": str(receipts)}
    return subprocess.run(
        ["bash", str(RUNNER), script], cwd=repo, env=env, capture_output=True, text=True
    )


def _receipts(repo: Path, receipts: Path) -> None:
    cwd = Path.cwd()
    os.chdir(repo)
    try:
        gate.check_receipts(receipts, [SCRIPT])
    finally:
        os.chdir(cwd)


def test_executed_assertion_leaves_a_receipt_the_gate_accepts(tmp_path: Path) -> None:
    repo = _repo(tmp_path, "echo asserted\n")
    receipts = tmp_path / "receipts"
    completed = _run(repo, receipts)
    assert completed.returncode == 0, completed.stderr
    assert "asserted" in completed.stdout
    _receipts(repo, receipts)


def test_failing_assertion_leaves_no_receipt_and_fails_the_gate(tmp_path: Path) -> None:
    repo = _repo(tmp_path, "exit 7\n")
    receipts = tmp_path / "receipts"
    assert _run(repo, receipts).returncode == 7
    with pytest.raises(gate.GateError, match="no pass receipt"):
        _receipts(repo, receipts)


def test_skipped_assertion_fails_the_gate(tmp_path: Path) -> None:
    repo = _repo(tmp_path, "echo asserted\n")
    with pytest.raises(gate.GateError, match="skipped, never started, or failed"):
        _receipts(repo, tmp_path / "never-written")


def test_assertion_that_cannot_start_fails_the_gate(tmp_path: Path) -> None:
    repo = _repo(tmp_path, "echo asserted\n")
    receipts = tmp_path / "receipts"
    missing = "charts/curie/ci/runtime/absent.sh"
    assert _run(repo, receipts, missing).returncode != 0
    assert not receipts.exists()


def test_receipt_for_a_different_blob_fails_the_gate(tmp_path: Path) -> None:
    repo = _repo(tmp_path, "echo asserted\n")
    receipts = tmp_path / "receipts"
    assert _run(repo, receipts).returncode == 0
    (repo / SCRIPT).write_text("echo changed after the run\n", encoding="utf-8")
    with pytest.raises(gate.GateError, match="not "):
        _receipts(repo, receipts)


def test_select_lists_added_and_modified_assertions_only(tmp_path: Path) -> None:
    repo = _repo(tmp_path, "echo one\n")
    other = repo / "charts/curie/ci/runtime/removed.sh"
    other.write_text("echo gone\n")
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t"]
    subprocess.run([*git, "add", "."], cwd=repo, check=True)
    subprocess.run([*git, "commit", "-qm", "base"], cwd=repo, check=True)
    (repo / SCRIPT).write_text("echo two\n")
    (repo / "charts/curie/ci/runtime/new-runtime.sh").write_text("echo new\n")
    (repo / "charts/curie/ci/runtime/notes.md").write_text("not a script\n")
    other.unlink()
    subprocess.run([*git, "add", "-A"], cwd=repo, check=True)
    subprocess.run([*git, "commit", "-qm", "head"], cwd=repo, check=True)
    completed = subprocess.run(
        [sys.executable, str(GATE_DIR / "gate.py"), "select", "--base", "HEAD~1", "--head", "HEAD"],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(completed.stdout) == [
        SCRIPT,
        "charts/curie/ci/runtime/new-runtime.sh",
    ]


def test_required_verdict_runs_the_gate_on_every_changed_assertion() -> None:
    jobs = yaml.safe_load(CI.read_text())["jobs"]
    steps = jobs["e2e-required"]["steps"]
    runs = "\n".join(str(step.get("run", "")) for step in steps)
    assert "registration --workflow .github/workflows/ci.yaml" in runs
    assert "receipts --dir" in runs
    assert "self-test --workflow .github/workflows/ci.yaml" in runs
    assert "runtime_assertions" in jobs["changes"]["outputs"]


def test_self_test_passes_on_the_real_workflow() -> None:
    gate.self_test(CI, "charts/curie/ci/runtime/publication-job-assertions.sh")


def test_self_test_refuses_an_unregistered_anchor() -> None:
    # A control anchored on an unregistered script would prove nothing.
    with pytest.raises(gate.GateError, match="unregistered"):
        gate.self_test(CI, "charts/curie/ci/runtime/metrics-alerts-runtime.sh")
