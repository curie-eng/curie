"""CI pin for the repository-toolchain proof job (issue #2611).

``runner/tests/test_repo_toolchain_proof.py`` skips every container leg when
the runner image is absent. The Python job does not build that image, so those
legs were a silent skip on the merge gate. This file pins the dedicated job
that builds the image and sets ``CURIE_REPO_TOOLCHAIN_PROOF`` to required.

The two negative controls are driven through the same assertion the real check
uses, rather than testing a matcher in isolation: dropping the required
setting, or building without loading the image into the daemon, fails the pin.
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
BUILD_STEP = "Build the runner image locally (gha layer cache)"
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

    assert "needs" not in job, (
        "repo-toolchain-proof must not serialise behind another job; it is "
        "the parallel half of the Python suite"
    )
    assert "if" not in job, (
        "a job-level if: makes a required check skip, which is not a pass (#1470)"
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

    assert BUILD_STEP in by_name, f"ci.yaml lost the {BUILD_STEP!r} step"
    build = by_name[BUILD_STEP]
    build_with = build.get("with") or {}
    assert build_with.get("file") == "runner/Dockerfile", (
        "the job must build runner/Dockerfile, not some other image"
    )
    assert build_with.get("load") is True, (
        "load must be true so the harness can docker-run the tag; a cache-only "
        "build leaves the image absent and the required-mode pytest would fail "
        "for the environment rather than for the recipe"
    )
    assert build_with.get("push") is False
    assert "curie-runner" in str(build_with.get("tags")), (
        "the built tag must match the harness default CURIE_RUNNER_IMAGE"
    )
    assert build_with.get("cache-from") == "type=gha,scope=runner"
    assert "cache-to" not in build_with, (
        "this job must not write the runner cache; the images matrix already "
        "refreshes scope=runner, and a second writer only contends"
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


def test_a_build_that_does_not_load_the_image_fails_the_pin() -> None:
    """Negative control: a cache-only build is an absent image on the job."""

    doc = _workflow()
    job = doc["jobs"][JOB_ID]
    build = next(step for step in job["steps"] if step.get("name") == BUILD_STEP)
    build["with"]["load"] = False
    with pytest.raises(AssertionError, match="load"):
        assert_proof_job_gates(doc)
