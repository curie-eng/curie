"""Registration and execution gate for cluster runtime assertions (#3391).

Every added or changed script under charts/curie/ci/runtime/ must be invoked,
through tools/runtime-assertion-gate/run.sh, by a step of a CI job that:

* is a dependency of the required `E2E required` job,
* is selected by the cluster tier or its registered chart scenarios,
* installs Calico before the invoking step, so NetworkPolicy is enforced, and
* runs the invoking step unconditionally or with its canonical chart scenario
  condition, with no continue-on-error.

`E2E required` then refuses the verdict unless each such script left a pass
receipt for its exact blob. The general cluster parity ladder is never taken as
proof that a new assertion ran.

Subcommands:
  select        print the runtime assertions a run must prove, as a JSON list
  registration  fail when a script has no qualifying invoking step
  receipts      fail when a script has no pass receipt for its current blob
  self-test     fail unless both checks reject a known-bad input (negative controls)
"""

from __future__ import annotations

import argparse
import copy
import json
import shlex
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

RUNTIME_DIR = "charts/curie/ci/runtime"
RUNNER = "tools/runtime-assertion-gate/run.sh"
REQUIRED_JOB = "e2e-required"
CLUSTER_SELECTOR = "${{ needs.changes.outputs.cluster == 'true' }}"
CHART_JOB = "e2e-cluster-chart-regressions"
CHART_SELECTOR = "${{ needs.changes.outputs.chart_scenarios != '[]' }}"
CALICO_MARKERS = ("projectcalico/calico", "rollout status daemonset/calico-node")


class GateError(Exception):
    """A runtime assertion is not registered or did not execute."""


def is_runtime_assertion(path: str) -> bool:
    parent, _, name = path.rpartition("/")
    return parent == RUNTIME_DIR and name.endswith(".sh")


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], capture_output=True, text=True, check=True).stdout


def changed_assertions(base: str, head: str) -> list[str]:
    """Added or modified runtime assertions between base and head."""
    out = _git("diff", "--no-renames", "--diff-filter=AM", "--name-only", f"{base}...{head}")
    return sorted(p for p in out.splitlines() if is_runtime_assertion(p))


def _load_jobs(workflow: Path) -> dict[str, Any]:
    document = yaml.safe_load(workflow.read_text(encoding="utf-8"))
    jobs = document.get("jobs") if isinstance(document, dict) else None
    if not isinstance(jobs, dict):
        raise GateError(f"{workflow}: no jobs mapping")
    return jobs


def _load_chart_registry(workflow: Path) -> dict[str, Any]:
    path = workflow.parent.parent / "e2e-selection.yaml"
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise GateError(f"{path}: cannot load the chart scenario registry") from exc
    section = document.get("chart_scenarios") if isinstance(document, dict) else None
    if (
        not isinstance(section, dict)
        or section.get("job") != CHART_JOB
        or not isinstance(section.get("scenarios"), dict)
    ):
        raise GateError(f"{path}: no chart scenario registry for {CHART_JOB}")
    return section


def _registered_chart_step(
    registry: dict[str, Any], step: dict[str, Any], script: str
) -> bool:
    for scenario_id, entry in registry.get("scenarios", {}).items():
        if not isinstance(scenario_id, str) or not isinstance(entry, dict):
            continue
        condition = (
            "${{ contains(fromJSON(needs.changes.outputs.chart_scenarios), '"
            + scenario_id
            + "') }}"
        )
        if (
            entry.get("step") == step.get("name")
            and isinstance(entry.get("inputs"), list)
            and script in entry["inputs"]
            and step.get("if") == condition
        ):
            return True
    return False


def _needs(job: dict[str, Any]) -> list[str]:
    needs = job.get("needs", [])
    return [needs] if isinstance(needs, str) else list(needs)


def _logical_lines(run: str) -> list[list[str]]:
    lines: list[list[str]] = []
    for line in run.replace("\\\n", " ").splitlines():
        try:
            words = shlex.split(line, comments=True)
        except ValueError:
            words = [line]
        if words:
            lines.append(words)
    return lines


def _invoked_script(run: str) -> list[str]:
    """Scripts a `run` block passes to the receipt runner as a top-level command."""
    return [
        words[2]
        for words in _logical_lines(run)
        if len(words) >= 3 and words[0] == "bash" and words[1] == RUNNER
    ]


def _runs_other_commands(run: str) -> bool:
    """True when the step does anything besides `set` options and runner calls.

    A dedicated step is what makes the invocation unconditional: the same line
    nested in a conditional, loop, or function would otherwise register.
    """
    return any(
        words[0] != "set" and not (len(words) >= 3 and words[:2] == ["bash", RUNNER])
        for words in _logical_lines(run)
    )


def _installs_calico(step: dict[str, Any], run: str) -> bool:
    return "if" not in step and all(marker in run for marker in CALICO_MARKERS)


@dataclass(frozen=True)
class Invocation:
    job: str
    step: str
    problems: tuple[str, ...]


def invocations(
    jobs: dict[str, Any], script: str, chart_registry: dict[str, Any]
) -> list[Invocation]:
    """Every step that passes `script` to the runner, with its disqualifiers."""
    required = jobs.get(REQUIRED_JOB)
    required_needs = set(_needs(required)) if isinstance(required, dict) else set()
    found: list[Invocation] = []
    for job_id, job in jobs.items():
        if not isinstance(job, dict):
            continue
        calico_seen = False
        for index, step in enumerate(job.get("steps") or []):
            if not isinstance(step, dict):
                continue
            run = step.get("run")
            run = run if isinstance(run, str) else ""
            if script in _invoked_script(run):
                problems: list[str] = []
                if job_id not in required_needs:
                    problems.append(f"job {job_id} is not a dependency of {REQUIRED_JOB}")
                selector = CHART_SELECTOR if job_id == CHART_JOB else CLUSTER_SELECTOR
                selected = (
                    job.get("if") == selector
                    if job_id == CHART_JOB
                    else str(job.get("if", "")).strip() == selector
                )
                if not selected:
                    problems.append(f"job {job_id} is not selected by {selector}")
                if not calico_seen:
                    problems.append(
                        f"job {job_id} does not install Calico before the step, "
                        "so NetworkPolicy is not enforced"
                    )
                if job_id == CHART_JOB:
                    if not _registered_chart_step(chart_registry, step, script):
                        problems.append(
                            "the step does not have its canonical chart scenario condition "
                            "and own script input in the registry"
                        )
                elif "if" in step:
                    problems.append("the step has an `if` and can be skipped")
                if _runs_other_commands(run):
                    problems.append(
                        "the step runs other commands; give the assertion a step of its own"
                    )
                if step.get("continue-on-error") or job.get("continue-on-error"):
                    problems.append("continue-on-error can hide a failure")
                name = str(step.get("name", f"step {index + 1}"))
                found.append(Invocation(job_id, name, tuple(problems)))
            if _installs_calico(step, run):
                calico_seen = True
    return found


def _check_registration(
    workflow: Path, jobs: dict[str, Any], scripts: list[str], chart_registry: dict[str, Any]
) -> None:
    errors: list[str] = []
    for script in scripts:
        found = invocations(jobs, script, chart_registry)
        if any(not item.problems for item in found):
            print(f"registered: {script}")
            continue
        if not found:
            errors.append(
                f"{script}: unregistered. Missing workflow step in {workflow}: "
                f"`bash {RUNNER} {script}` in a Calico cluster job that "
                f"{REQUIRED_JOB} needs"
            )
            continue
        for item in found:
            errors.append(
                f"{script}: step {item.step!r} in job {item.job} does not count: "
                + "; ".join(item.problems)
            )
    if errors:
        raise GateError("\n".join(errors))


def check_registration(workflow: Path, scripts: list[str]) -> None:
    jobs = _load_jobs(workflow)
    chart_registry = _load_chart_registry(workflow) if CHART_JOB in jobs else {}
    _check_registration(workflow, jobs, scripts, chart_registry)


def check_receipts(receipts: Path, scripts: list[str]) -> None:
    errors: list[str] = []
    for script in scripts:
        receipt = receipts / f"{Path(script).stem}.json"
        if not receipt.is_file():
            errors.append(
                f"{script}: selected but no pass receipt; it was skipped, never started, or failed"
            )
            continue
        data = json.loads(receipt.read_text(encoding="utf-8"))
        blob = _git("hash-object", "--", script).strip()
        if data.get("script") != script or data.get("status") != "pass":
            errors.append(f"{script}: receipt does not record a pass for this script")
        elif data.get("blob") != blob:
            errors.append(f"{script}: receipt is for blob {data.get('blob')}, not {blob}")
        else:
            print(f"executed: {script} ({blob})")
    if errors:
        raise GateError("\n".join(errors))


NEGATIVE_UNREGISTERED = f"{RUNTIME_DIR}/unregistered-negative-control.sh"


def self_test(workflow: Path, registered: str) -> None:
    """Prove the gate fails closed against the real workflow.

    An unregistered script must fail registration, and a registered script with
    no receipt must fail the execution check. A chart registry copy that omits
    an invoked script from its own inputs must fail registration too. The
    registered scripts must pass registration, or the controls prove nothing.
    """
    check_registration(workflow, [registered])
    jobs = _load_jobs(workflow)
    chart_registry = _load_chart_registry(workflow)
    chart_script = next(
        (
            script
            for entry in chart_registry["scenarios"].values()
            if isinstance(entry, dict)
            for script in entry.get("inputs", [])
            if isinstance(script, str) and is_runtime_assertion(script)
        ),
        None,
    )
    if chart_script is None:
        raise GateError("the chart registry has no runtime assertion for its negative control")
    _check_registration(workflow, jobs, [chart_script], chart_registry)
    omitted_registry = copy.deepcopy(chart_registry)
    for entry in omitted_registry["scenarios"].values():
        if isinstance(entry, dict) and isinstance(entry.get("inputs"), list):
            entry["inputs"] = [script for script in entry["inputs"] if script != chart_script]
    controls: tuple[tuple[str, Callable[[], None]], ...] = (
        ("unregistered script", lambda: check_registration(workflow, [NEGATIVE_UNREGISTERED])),
        ("unexecuted script", lambda: check_receipts(Path("/nonexistent-receipts"), [registered])),
        (
            "chart registry script omission",
            lambda: _check_registration(workflow, jobs, [chart_script], omitted_registry),
        ),
    )
    for label, attempt in controls:
        try:
            attempt()
        except GateError as exc:
            print(f"negative control rejected the {label}: {exc}")
        else:
            raise GateError(f"negative control passed an {label}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    select = sub.add_parser("select")
    select.add_argument("--base", required=True)
    select.add_argument("--head", required=True)
    registration = sub.add_parser("registration")
    registration.add_argument("--workflow", required=True, type=Path)
    registration.add_argument("--scripts", required=True, help="JSON list")
    receipts = sub.add_parser("receipts")
    receipts.add_argument("--dir", required=True, type=Path)
    receipts.add_argument("--scripts", required=True, help="JSON list")
    control = sub.add_parser("self-test")
    control.add_argument("--workflow", required=True, type=Path)
    control.add_argument("--registered", required=True)
    return parser


def _scripts(raw: str) -> list[str]:
    value = json.loads(raw)
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise GateError("--scripts must be a JSON list of paths")
    bad = [v for v in value if not is_runtime_assertion(v)]
    if bad:
        raise GateError(f"not runtime assertions: {', '.join(bad)}")
    return value


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "select":
            print(json.dumps(changed_assertions(args.base, args.head)))
        elif args.command == "registration":
            check_registration(args.workflow, _scripts(args.scripts))
        elif args.command == "receipts":
            check_receipts(args.dir, _scripts(args.scripts))
        else:
            self_test(args.workflow, args.registered)
    except GateError as exc:
        print(f"runtime assertion gate failed:\n{exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
