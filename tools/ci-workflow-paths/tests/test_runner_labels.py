"""Every GitHub Actions runner label in the workflows is pinned.

A floating `*-latest` label lets GitHub swap the runner OS under scheduled runs
with no change in this repository: `ubuntu-latest` became Ubuntu 26.04 on
2026-10-20, which broke kind image loads and a helm flag in the scheduled kind
run. A pinned label moves only when a commit here moves it.

The helper resolves the labels a job can run on, including `${{ matrix.<key> }}`
expressions, and fails closed on any expression it cannot resolve: a gate that
skips what it cannot read would let the next indirection hide a floating label.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"

MATRIX_EXPRESSION = re.compile(r"^\$\{\{\s*matrix\.([A-Za-z0-9_-]+)\s*\}\}$")


def _matrix_values(job_id: str, job: dict[str, Any], key: str) -> Iterator[str]:
    """Every value `matrix.<key>` can take: the list under the key plus each include."""
    matrix = (job.get("strategy") or {}).get("matrix")
    if not isinstance(matrix, dict):
        raise ValueError(f"job {job_id!r}: runs-on uses matrix.{key} but has no matrix mapping")
    found: list[Any] = []
    listed = matrix.get(key)
    if listed is not None:
        if not isinstance(listed, list):
            raise ValueError(f"job {job_id!r}: matrix.{key} is not a literal list: {listed!r}")
        found.extend(listed)
    include = matrix.get("include") or []
    if not isinstance(include, list):
        raise ValueError(f"job {job_id!r}: matrix.include is not a literal list: {include!r}")
    found.extend(entry[key] for entry in include if isinstance(entry, dict) and key in entry)
    if not found:
        raise ValueError(f"job {job_id!r}: matrix.{key} resolves to no values")
    for value in found:
        if not isinstance(value, str):
            raise ValueError(f"job {job_id!r}: matrix.{key} value is not a string: {value!r}")
        yield value


def _resolve(job_id: str, job: dict[str, Any], label: Any) -> Iterator[str]:
    if not isinstance(label, str):
        raise ValueError(f"job {job_id!r}: runner label is not a string: {label!r}")
    expression = MATRIX_EXPRESSION.match(label.strip())
    if expression:
        yield from _matrix_values(job_id, job, expression.group(1))
    elif "${{" in label:
        raise ValueError(f"job {job_id!r}: cannot resolve runner expression {label!r}")
    else:
        yield label


def runner_labels(workflow: dict[str, Any]) -> Iterator[tuple[str, str]]:
    """Yield (job_id, label) for every runner label a job can run on.

    Raises ValueError for a `runs-on` shape or expression it cannot resolve.
    """
    for job_id, job in (workflow.get("jobs") or {}).items():
        runs_on = job.get("runs-on")
        if runs_on is None:
            continue
        if isinstance(runs_on, dict):
            if "labels" not in runs_on:
                raise ValueError(f"job {job_id!r}: runs-on mapping has no labels: {runs_on!r}")
            runs_on = runs_on["labels"]
        for label in runs_on if isinstance(runs_on, list) else [runs_on]:
            for resolved in _resolve(job_id, job, label):
                yield job_id, resolved


def floating_labels(workflow: dict[str, Any]) -> list[tuple[str, str]]:
    return [(job, label) for job, label in runner_labels(workflow) if label.endswith("-latest")]


def _workflow_paths() -> list[Path]:
    # Both extensions: GitHub Actions honours `.yml` exactly as it honours `.yaml`.
    paths = sorted([*WORKFLOWS_DIR.glob("*.yaml"), *WORKFLOWS_DIR.glob("*.yml")])
    assert paths, f"no workflow files found under {WORKFLOWS_DIR}"
    return paths


def test_no_workflow_uses_a_floating_runner_label() -> None:
    offenders: list[str] = []
    for path in _workflow_paths():
        workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(workflow, dict), f"{path.name} did not parse as a YAML mapping"
        labels = list(runner_labels(workflow))
        # A job whose labels resolved to nothing would pass this gate vacuously.
        resolved_jobs = {job_id for job_id, _ in labels}
        for job_id, job in (workflow.get("jobs") or {}).items():
            if job.get("runs-on") is not None:
                assert job_id in resolved_jobs, f"{path.name}: job {job_id!r} resolved no labels"
        offenders.extend(
            f"{path.name}: job {job_id!r} runs on floating label {label!r}"
            for job_id, label in labels
            if label.endswith("-latest")
        )
    assert not offenders, (
        "pin every runner label to a version (for example ubuntu-24.04, macos-26); "
        "a `*-latest` label changes OS under scheduled runs with no commit here:\n"
        + "\n".join(offenders)
    )


@pytest.mark.parametrize(
    ("workflow", "expected"),
    [
        pytest.param(
            {"jobs": {"build": {"runs-on": "ubuntu-latest"}}},
            [("build", "ubuntu-latest")],
            id="plain-string",
        ),
        pytest.param(
            {"jobs": {"build": {"runs-on": ["self-hosted", "macos-latest"]}}},
            [("build", "macos-latest")],
            id="list-element",
        ),
        pytest.param(
            {"jobs": {"build": {"runs-on": {"labels": ["macos-latest"], "group": "pool"}}}},
            [("build", "macos-latest")],
            id="mapping-labels",
        ),
        pytest.param(
            {
                "jobs": {
                    "image": {
                        "runs-on": "${{ matrix.runner }}",
                        "strategy": {
                            "matrix": {
                                "include": [
                                    {"platform": "linux/amd64", "runner": "ubuntu-latest"},
                                    {"platform": "linux/arm64", "runner": "ubuntu-24.04-arm"},
                                ]
                            }
                        },
                    }
                }
            },
            [("image", "ubuntu-latest")],
            id="matrix-include-entry",
        ),
        pytest.param(
            {
                "jobs": {
                    "cli": {
                        "runs-on": "${{ matrix.os }}",
                        "strategy": {"matrix": {"os": ["ubuntu-24.04", "windows-latest"]}},
                    }
                }
            },
            [("cli", "windows-latest")],
            id="matrix-list",
        ),
    ],
)
def test_floating_labels_are_reported(
    workflow: dict[str, Any], expected: list[tuple[str, str]]
) -> None:
    assert floating_labels(workflow) == expected


def test_pinned_labels_report_nothing() -> None:
    workflow = {
        "jobs": {
            "lint": {"runs-on": "ubuntu-24.04"},
            "image": {
                "runs-on": "${{ matrix.runner }}",
                "strategy": {
                    "matrix": {
                        "include": [
                            {"platform": "linux/amd64", "runner": "ubuntu-24.04"},
                            {"platform": "linux/arm64", "runner": "ubuntu-24.04-arm"},
                        ]
                    }
                },
            },
            "portability": {"runs-on": ["macos-26"]},
        }
    }
    # The helper must see every pinned label, so "reports nothing" is not a blind helper.
    assert sorted(runner_labels(workflow)) == [
        ("image", "ubuntu-24.04"),
        ("image", "ubuntu-24.04-arm"),
        ("lint", "ubuntu-24.04"),
        ("portability", "macos-26"),
    ]
    assert floating_labels(workflow) == []


@pytest.mark.parametrize(
    "job",
    [
        pytest.param({"runs-on": "${{ inputs.runner }}"}, id="unsupported-context"),
        pytest.param(
            {
                "runs-on": "${{ matrix.runner }}",
                "strategy": {"matrix": "${{ fromJson(needs.plan.outputs.matrix) }}"},
            },
            id="dynamic-matrix",
        ),
        pytest.param({"runs-on": "${{ matrix.runner }}"}, id="matrix-missing"),
        pytest.param({"runs-on": "prefix-${{ matrix.runner }}"}, id="embedded-expression"),
    ],
)
def test_unresolvable_runner_expression_fails_closed(job: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        list(runner_labels({"jobs": {"build": job}}))
