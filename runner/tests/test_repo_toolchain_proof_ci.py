"""CI pin for the repository-toolchain proof job (issue #2611).

``runner/tests/test_repo_toolchain_proof.py`` skips every container leg when
the runner image is absent. The Python job does not build that image, so those
legs were a silent skip on the merge gate. This file pins the dedicated job
that loads the image and sets ``CURIE_REPO_TOOLCHAIN_PROOF`` to required.

The two negative controls are driven through the same assertion the real check
uses, rather than testing a matcher in isolation: dropping the required
setting, or dropping the step that loads the image into the daemon, fails the pin.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CI_YAML = REPO_ROOT / ".github" / "workflows" / "ci.yaml"
JOB_ID = "repo-toolchain-proof"
REQUIRED_ASSIGNMENT = "CURIE_REPO_TOOLCHAIN_PROOF: required"
HARNESS = "runner/tests/test_repo_toolchain_proof.py"
LOAD_STEP = "Load the runner image built by the ci-images job"
TAG_STEP = "Tag the runner image as the harness default (curie-runner)"
PROOF_STEP = "Repository toolchain proof"
ABSENT_IMAGE_STEP = "Absent image fails in required mode (negative control)"
REQUIRED_STEP = "Required setting is present (negative control)"
ABSENT_IMAGE = "curie-runner-absent-2611"


def _workflow(raw: str | None = None) -> dict[str, Any]:
    return yaml.safe_load(raw if raw is not None else CI_YAML.read_text())


def assert_proof_job_gates(doc: dict[str, Any]) -> None:
    """The contract the dedicated CI job must keep, or the #2571 skip returns."""

    jobs = doc.get("jobs") or {}
    assert JOB_ID in jobs, (
        "ci.yaml must keep the repo-toolchain-proof job; without it the "
        "container legs of test_repo_toolchain_proof.py skip on the merge gate"
    )
    job = jobs[JOB_ID]
    assert isinstance(job, dict)

    python = jobs.get("python") or {}
    python_env = python.get("env") or {}
    assert python_env.get("CURIE_REPO_TOOLCHAIN_PROOF") != "required", (
        "the Python job must not set CURIE_REPO_TOOLCHAIN_PROOF=required; it "
        "does not build curie-runner, so required mode would fail that job on "
        "every run instead of gating in the dedicated job"
    )

    needs = job.get("needs")
    if isinstance(needs, str):
        needs = [needs]
    assert needs == ["changes", "ci-images"], (
        "repo-toolchain-proof may wait on the path selector and the shared "
        "image build only; it must not serialise behind the Python suite"
    )
    assert job.get("if") == "${{ needs.changes.outputs.images == 'true' }}", (
        "repo-toolchain-proof runs when images are selected; a skip here is "
        "not a merge-required check"
    )

    env = job.get("env") or {}
    assert env.get("CURIE_REPO_TOOLCHAIN_PROOF") == "required", (
        "CURIE_REPO_TOOLCHAIN_PROOF must be required so an absent image fails "
        "this job instead of skipping"
    )

    steps = job.get("steps") or []
    by_name = {step.get("name"): step for step in steps if step.get("name")}

    assert REQUIRED_STEP in by_name, (
        "the job must keep the negative control that the required setting is present"
    )
    assert ABSENT_IMAGE_STEP in by_name, (
        "the job must keep the negative control that an absent image fails in required mode"
    )
    absent = by_name[ABSENT_IMAGE_STEP]
    absent_env = absent.get("env") or {}
    assert absent_env.get("CURIE_RUNNER_IMAGE") == ABSENT_IMAGE, (
        "the absent-image control must inspect a tag this job never builds"
    )
    assert HARNESS in (absent.get("run") or ""), (
        "the absent-image control must drive the real harness, not a stub"
    )
    assert "may not skip" in (absent.get("run") or ""), (
        "the absent-image control must assert the required-mode failure, not any non-zero exit"
    )

    assert LOAD_STEP in by_name, f"ci.yaml lost the {LOAD_STEP!r} step"
    load = by_name[LOAD_STEP]
    assert load.get("uses") == "./.github/actions/load-ci-images", (
        "the job must load the runner image the ci-images job built"
    )
    assert (load.get("with") or {}).get("images") == "runner", (
        "the job must load the runner image, not some other image"
    )
    assert TAG_STEP in by_name, f"ci.yaml lost the {TAG_STEP!r} step"
    tag_run = by_name[TAG_STEP].get("run") or ""
    assert tag_run.split()[-1:] == ["curie-runner"] and "curie-ci/runner:candidate" in tag_run, (
        "the loaded image must be tagged as the harness default CURIE_RUNNER_IMAGE"
    )
    names = [step.get("name") for step in steps]
    assert names.index(LOAD_STEP) < names.index(TAG_STEP) < names.index(PROOF_STEP), (
        "the image must be loaded and tagged before the proof runs"
    )

    assert PROOF_STEP in by_name, f"ci.yaml lost the {PROOF_STEP!r} step"
    proof = by_name[PROOF_STEP]
    assert HARNESS in (proof.get("run") or ""), (
        "the proof step must run test_repo_toolchain_proof.py"
    )
    proof_env = proof.get("env") or {}
    step_mode = proof_env.get("CURIE_REPO_TOOLCHAIN_PROOF")
    assert step_mode in (None, "required"), (
        "the proof step must inherit the job-level required setting; a "
        f"step-level override of {step_mode!r} would drop the gate"
    )


def test_ci_job_builds_the_runner_image_and_requires_the_harness() -> None:
    assert_proof_job_gates(_workflow())


def test_dropping_required_from_the_job_fails_the_pin() -> None:
    """Negative control: the required setting cannot be dropped silently."""

    raw = CI_YAML.read_text()
    assert raw.count(REQUIRED_ASSIGNMENT) == 1, (
        "the YAML assignment of CURIE_REPO_TOOLCHAIN_PROOF: required moved or "
        "was duplicated; update this control so it still doctors the one job env"
    )
    doctored = _workflow(raw.replace(REQUIRED_ASSIGNMENT, "CURIE_REPO_TOOLCHAIN_PROOF: optional"))
    with pytest.raises(AssertionError, match="required"):
        assert_proof_job_gates(doctored)


def test_a_job_that_does_not_load_the_image_fails_the_pin() -> None:
    """Negative control: dropping the load step leaves the image absent."""

    doc = _workflow()
    job = doc["jobs"][JOB_ID]
    job["steps"] = [step for step in job["steps"] if step.get("name") != LOAD_STEP]
    with pytest.raises(AssertionError, match="Load the runner image"):
        assert_proof_job_gates(doc)
