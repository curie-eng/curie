"""Negative and positive controls for the runtime assertion gate (#3391)."""

from __future__ import annotations

import fnmatch
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


def _needs(job: dict[str, object]) -> list[str]:
    needs = job.get("needs", [])
    return [needs] if isinstance(needs, str) else list(needs)  # type: ignore[arg-type]


def _runs(job: dict[str, object]) -> str:
    return "\n".join(str(step.get("run", "")) for step in job["steps"])  # type: ignore[attr-defined]


def test_required_verdict_runs_the_gate_on_every_changed_assertion() -> None:
    jobs = yaml.safe_load(CI.read_text())["jobs"]
    registration = jobs["runtime-assertion-registration"]
    required = jobs["e2e-required"]
    registration_runs = _runs(registration)
    assert "self-test --workflow .github/workflows/ci.yaml" in registration_runs
    assert "registration --workflow .github/workflows/ci.yaml" in registration_runs
    assert _needs(registration) == ["changes"]
    assert "runtime-assertion-registration" in _needs(required)
    required_runs = _runs(required)
    assert "receipts --dir" in required_runs
    assert "registration --workflow" not in required_runs
    assert "self-test --workflow" not in required_runs
    assert "runtime_assertions" in jobs["changes"]["outputs"]


def test_registration_job_and_step_conditions_skip_only_the_empty_list() -> None:
    job = yaml.safe_load(CI.read_text())["jobs"]["runtime-assertion-registration"]
    # The job runs on every event and every verdict.
    assert "if" not in job
    steps = {step.get("name"): step for step in job["steps"]}
    # Negative controls run on every verdict (#3391).
    assert "if" not in steps["Runtime assertion gate negative controls"]
    # Skipped only for the empty list; any nonempty list runs the guard.
    guard = steps["Changed runtime assertions are registered"]
    assert guard["if"] == "needs.changes.outputs.runtime_assertions != '[]'"


def _run_registration_guard(
    scripts: list[str], cluster_selected: str
) -> subprocess.CompletedProcess[str]:
    job = yaml.safe_load(CI.read_text())["jobs"]["runtime-assertion-registration"]
    name = "Changed runtime assertions are registered"
    steps = [s for s in job["steps"] if s.get("name") == name]
    assert len(steps) == 1
    environment = os.environ.copy()
    environment.update(
        {"RUNTIME_ASSERTIONS": json.dumps(scripts), "CLUSTER_SELECTED": cluster_selected}
    )
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", steps[0]["run"]],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def test_registration_guard_fails_an_unregistered_changed_assertion() -> None:
    result = _run_registration_guard(
        ["charts/curie/ci/runtime/unregistered-negative-control.sh"], "true"
    )
    assert result.returncode != 0
    assert "unregistered" in result.stdout + result.stderr


def test_registration_guard_fails_when_the_cluster_tier_was_not_selected() -> None:
    result = _run_registration_guard(
        ["charts/curie/ci/runtime/unregistered-negative-control.sh"], "false"
    )
    assert result.returncode != 0
    assert "cluster tier" in result.stdout + result.stderr


def test_registration_guard_passes_a_registered_changed_assertion() -> None:
    result = _run_registration_guard(
        ["charts/curie/ci/runtime/publication-job-assertions.sh"], "true"
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_self_test_passes_on_the_real_workflow() -> None:
    gate.self_test(CI, "charts/curie/ci/runtime/publication-job-assertions.sh")


def test_self_test_refuses_an_unregistered_anchor() -> None:
    # A control anchored on an unregistered script would prove nothing.
    with pytest.raises(gate.GateError, match="unregistered"):
        gate.self_test(CI, "charts/curie/ci/runtime/metrics-alerts-runtime.sh")


CHART_JOB = "e2e-cluster-chart-regressions"
CHART_JOB_IF = "${{ needs.changes.outputs.chart_scenarios != '[]' }}"
CHART_STEP_IF = (
    "${{ matrix.leg == 'chart' && "
    "contains(fromJSON(needs.changes.outputs.chart_scenarios), 'example-runtime') }}"
)
CHART_RUNTIME_SCRIPTS = [
    "charts/curie/ci/runtime/upgrade-drain-installation-runtime.sh",
    "charts/curie/ci/runtime/e2e-connector-identity-runtime.sh",
    "charts/curie/ci/runtime/runner-resources-assertions.sh",
    "charts/curie/ci/runtime/langfuse-postgres-readiness-runtime.sh",
    "charts/curie/ci/runtime/connector-readiness-runtime.sh",
    "charts/curie/ci/runtime/workspace-dependency-diagnostic-runtime.sh",
    "charts/curie/ci/runtime/idle-capacity-reclamation-runtime.sh",
]


def _chart_workflow(tmp_path: Path) -> tuple[Path, Path]:
    workflow = tmp_path / ".github/workflows/ci.yaml"
    workflow.parent.mkdir(parents=True)
    registry = tmp_path / ".github/e2e-selection.yaml"
    document = yaml.safe_load((REPO_ROOT / ".github/e2e-selection.yaml").read_text())
    document["chart_scenarios"] = {
        "job": CHART_JOB,
        "shared_inputs": ["charts/curie/templates"],
        "scenarios": {"example-runtime": {"step": "Run it", "inputs": [SCRIPT], "leg": "chart"}},
    }
    registry.write_text(yaml.safe_dump(document, sort_keys=False))
    workflow.write_text(yaml.safe_dump({
        "jobs": {
            CHART_JOB: {
                "if": CHART_JOB_IF,
                "steps": [
                    {"name": "Install Calico", "run": CALICO},
                    {"name": "Run it", "if": CHART_STEP_IF, "run": INVOKE},
                ],
            },
            "e2e-required": {"needs": ["changes", CHART_JOB], "steps": []},
        }
    }, sort_keys=False))
    return workflow, registry


def test_chart_assertion_registers_with_its_own_scenario_condition(tmp_path: Path) -> None:
    workflow, _registry = _chart_workflow(tmp_path)
    gate.check_registration(workflow, [SCRIPT])


@pytest.mark.parametrize(
    "mutation",
    ["old-job-if", "wrong-job-if", "wrong-step-if", "wrong-scenario-id", "missing-step-if",
     "missing-entry", "wrong-step-name", "script-only-shared", "script-only-other-scenario",
     "no-calico", "late-calico", "conditional-calico", "step-continue", "job-continue",
     "other-command", "not-required", "different-job", "missing-leg-clause", "wrong-leg",
     "missing-registry-leg"],
)
def test_chart_scenario_registration_preserves_all_gate_requirements(
    tmp_path: Path, mutation: str,
) -> None:
    workflow, registry_path = _chart_workflow(tmp_path)
    document = yaml.safe_load(workflow.read_text())
    registry = yaml.safe_load(registry_path.read_text())
    job = document["jobs"][CHART_JOB]
    step = job["steps"][1]
    section = registry["chart_scenarios"]
    entry = section["scenarios"]["example-runtime"]
    if mutation == "old-job-if":
        job["if"] = "${{ needs.changes.outputs.cluster == 'true' }}"
    elif mutation == "wrong-job-if":
        job["if"] = "${{ needs.changes.outputs.chart_scenarios != '[]' && false }}"
    elif mutation == "wrong-step-if":
        step["if"] = "${{ false }}"
    elif mutation == "wrong-scenario-id":
        step["if"] = CHART_STEP_IF.replace("example-runtime", "other-runtime")
    elif mutation == "missing-step-if":
        del step["if"]
    elif mutation == "missing-entry":
        del section["scenarios"]["example-runtime"]
    elif mutation == "wrong-step-name":
        entry["step"] = "Another step"
    elif mutation in {"script-only-shared", "script-only-other-scenario"}:
        entry["inputs"] = ["charts/curie/ci/runtime/other-runtime.sh"]
        if mutation == "script-only-shared":
            section["shared_inputs"].append(SCRIPT)
        else:
            section["scenarios"]["other-runtime"] = {
                "step": "Other step",
                "inputs": [SCRIPT],
                "leg": "chart",
            }
    elif mutation == "no-calico":
        del job["steps"][0]
    elif mutation == "late-calico":
        job["steps"].append(job["steps"].pop(0))
    elif mutation == "conditional-calico":
        job["steps"][0]["if"] = "${{ false }}"
    elif mutation == "step-continue":
        step["continue-on-error"] = True
    elif mutation == "job-continue":
        job["continue-on-error"] = True
    elif mutation == "other-command":
        step["run"] += "\necho unrelated command"
    elif mutation == "not-required":
        document["jobs"]["e2e-required"]["needs"] = ["changes"]
    elif mutation == "missing-leg-clause":
        # The pre-#4722 condition runs the step in every leg.
        step["if"] = (
            "${{ contains(fromJSON(needs.changes.outputs.chart_scenarios), 'example-runtime') }}"
        )
    elif mutation == "wrong-leg":
        step["if"] = CHART_STEP_IF.replace("'chart'", "'workload'")
    elif mutation == "missing-registry-leg":
        del entry["leg"]
    else:
        document["jobs"]["other-chart-job"] = document["jobs"].pop(CHART_JOB)
        document["jobs"]["e2e-required"]["needs"] = ["changes", "other-chart-job"]
    workflow.write_text(yaml.safe_dump(document, sort_keys=False))
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False))
    with pytest.raises(gate.GateError):
        gate.check_registration(workflow, [SCRIPT])


def test_real_ci_registers_every_enabled_chart_runtime_script() -> None:
    gate.check_registration(CI, list(CHART_RUNTIME_SCRIPTS))


def test_chart_legs_upload_distinct_receipts_the_required_verdict_merges() -> None:
    jobs = yaml.safe_load(CI.read_text())["jobs"]
    uploads = [
        step
        for step in jobs[CHART_JOB]["steps"]
        if step.get("name") == "Upload runtime assertion receipts"
    ]
    assert len(uploads) == 1
    name = uploads[0]["with"]["name"]
    assert name == "runtime-assertion-receipts-${{ github.job }}-${{ matrix.leg }}"
    downloads = [
        step
        for step in jobs["e2e-required"]["steps"]
        if step.get("name") == "Download runtime assertion receipts"
    ]
    assert len(downloads) == 1
    download = downloads[0]["with"]
    assert download["merge-multiple"] is True
    names = {
        name.replace("${{ github.job }}", CHART_JOB).replace("${{ matrix.leg }}", leg)
        for leg in ("chart", "workload")
    }
    # Two legs uploading one name would collide; each leg needs its own artifact.
    assert len(names) == 2
    for artifact in names:
        assert "${{" not in artifact
        assert fnmatch.fnmatchcase(artifact, download["pattern"]), artifact


def test_self_test_exercises_the_registry_omission_negative_control(
    capsys: pytest.CaptureFixture[str],
) -> None:
    gate.self_test(CI, "charts/curie/ci/runtime/publication-job-assertions.sh")
    output = capsys.readouterr().out
    assert "negative control rejected" in output
    assert "registry" in output.lower()
