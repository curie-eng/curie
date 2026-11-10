"""Structural contract for release image signing, SBOM, provenance, and chart pinning (#3843).

Every published image carries a per-platform SBOM and max-mode SLSA provenance
from BuildKit, every multi-arch index is signed keylessly with cosign and gets a
pushed GitHub build-provenance attestation, and the release chart is packaged
only after it has verified those signatures and pinned each first-party image
to its verified digest.
"""

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "release.yaml"

SHA_PIN = re.compile(r"@[0-9a-f]{40}$")
INDEX_DIGEST = "${{ steps.index.outputs.digest }}"
OWNER = "${{ github.repository_owner }}"

BUILD_JOBS = ["build", "worker-local-build", "dark-factory-runner-build"]
MERGE_JOBS = {
    "merge": "curie-${{ matrix.name }}",
    "worker-local-merge": "curie-worker-local",
    "dark-factory-runner-merge": "curie-dark-factory-runner",
}
CHART_IMAGES = ["api", "dispatcher", "mail-adapter", "worker", "ui", "runner"]


def load_workflow() -> dict:
    # BaseLoader keeps `on:` a string key and every scalar a string; anchors
    # and aliases still resolve.
    return yaml.load(WORKFLOW_PATH.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


@pytest.fixture(scope="module")
def jobs() -> dict:
    return load_workflow()["jobs"]


def steps_of(job: dict) -> list[dict]:
    return job["steps"]


def uses_of(step: dict) -> str:
    return step.get("uses", "")


def run_of(step: dict) -> str:
    return step.get("run", "")


def index_where(steps: list[dict], predicate, what: str) -> int:
    matches = [i for i, step in enumerate(steps) if predicate(step)]
    assert matches, f"no step {what}"
    return matches[0]


def named(name: str):
    return lambda step: step.get("name") == name


def as_list(value) -> list[str]:
    return [value] if isinstance(value, str) else list(value)


def is_true(value) -> bool:
    return str(value).strip().lower() == "true"


def not_soft_failing(step: dict) -> None:
    assert not is_true(step.get("continue-on-error", "false")), (
        f"{step.get('name')} must fail the job, not continue on error"
    )


class TestPerArchBuildsCarrySbomAndProvenance:
    @pytest.mark.parametrize("job_name", BUILD_JOBS)
    def test_build_push_step_emits_sbom_and_max_provenance(self, jobs, job_name):
        steps = steps_of(jobs[job_name])
        build_steps = [s for s in steps if uses_of(s).startswith("docker/build-push-action@")]
        assert len(build_steps) == 1, job_name
        with_ = build_steps[0].get("with", {})
        assert is_true(with_.get("sbom")), f"{job_name}: sbom must be true"
        assert str(with_.get("provenance", "")).strip() == "mode=max", (
            f"{job_name}: provenance must be mode=max"
        )


class TestMergeJobsSignAndAttestTheIndex:
    @pytest.mark.parametrize("job_name", list(MERGE_JOBS))
    def test_permissions_allow_keyless_signing_and_attestation(self, jobs, job_name):
        permissions = jobs[job_name].get("permissions", {})
        assert permissions.get("id-token") == "write", job_name
        assert permissions.get("attestations") == "write", job_name
        assert permissions.get("packages") == "write", job_name

    @pytest.mark.parametrize("job_name", list(MERGE_JOBS))
    def test_index_digest_is_resolved_after_the_manifest_is_pushed(self, jobs, job_name):
        image = MERGE_JOBS[job_name]
        steps = steps_of(jobs[job_name])
        create = index_where(
            steps, named("Create manifest list and push"), "Create manifest list and push"
        )
        resolve = index_where(steps, lambda s: s.get("id") == "index", "with id `index`")
        assert resolve > create, f"{job_name}: digest resolved before the index exists"
        script = run_of(steps[resolve])
        assert "imagetools create" in script and "--dry-run" in script, job_name
        assert "sha256sum" in script, job_name
        assert "imagetools inspect" in script, job_name
        assert "@${digest}" in script or "@$digest" in script, job_name
        assert f"ghcr.io/{OWNER}/{image}" in script, job_name
        assert "digest=" in script and "GITHUB_OUTPUT" in script, job_name
        assert steps[resolve].get("working-directory") == "${{ runner.temp }}/digests", job_name
        assert "sha-${GITHUB_SHA}" not in script and ":sha-" not in script, (
            f"{job_name}: must not re-read the shared sha tag"
        )
        not_soft_failing(steps[resolve])

    @pytest.mark.parametrize("job_name", list(MERGE_JOBS))
    def test_index_is_signed_with_cosign_by_digest(self, jobs, job_name):
        image_ref = f"ghcr.io/{OWNER}/{MERGE_JOBS[job_name]}@{INDEX_DIGEST}"
        steps = steps_of(jobs[job_name])
        create = index_where(
            steps, named("Create manifest list and push"), "Create manifest list and push"
        )
        resolve = index_where(steps, lambda s: s.get("id") == "index", "with id `index`")
        install = index_where(
            steps,
            lambda s: uses_of(s).startswith("sigstore/cosign-installer@"),
            "installing cosign",
        )
        assert SHA_PIN.search(uses_of(steps[install])), (
            "cosign-installer must be pinned to a full SHA"
        )
        sign = index_where(
            steps,
            lambda s: "cosign sign --yes" in run_of(s) and image_ref in run_of(s),
            f"running `cosign sign --yes` on {image_ref}",
        )
        assert "sign-blob" not in run_of(steps[sign])
        assert sign > create and sign > resolve and sign > install, job_name
        not_soft_failing(steps[sign])

    @pytest.mark.parametrize("job_name", list(MERGE_JOBS))
    def test_index_gets_a_pushed_build_provenance_attestation(self, jobs, job_name):
        steps = steps_of(jobs[job_name])
        create = index_where(
            steps, named("Create manifest list and push"), "Create manifest list and push"
        )
        resolve = index_where(steps, lambda s: s.get("id") == "index", "with id `index`")
        attest = index_where(
            steps,
            lambda s: uses_of(s).startswith("actions/attest-build-provenance@"),
            "using actions/attest-build-provenance",
        )
        assert SHA_PIN.search(uses_of(steps[attest])), (
            "attest-build-provenance must be pinned to a full SHA"
        )
        with_ = steps[attest].get("with", {})
        assert with_.get("subject-name") == f"ghcr.io/{OWNER}/{MERGE_JOBS[job_name]}", job_name
        assert with_.get("subject-digest") == INDEX_DIGEST, job_name
        assert is_true(with_.get("push-to-registry")), job_name
        assert attest > create and attest > resolve, job_name
        not_soft_failing(steps[attest])


class TestChartPinsVerifiedDigests:
    def test_chart_waits_for_every_image_merge(self, jobs):
        chart = jobs["chart"]
        assert "merge" in as_list(chart["needs"])
        assert "authorize-release" in as_list(chart["needs"])
        condition = chart["if"]
        assert "needs.merge.result == 'success'" in condition
        assert "needs.authorize-release.result == 'success'" in condition
        assert "startsWith(github.ref, 'refs/tags/v')" in condition

    def test_signatures_are_verified_then_digests_pinned_before_packaging(self, jobs):
        steps = steps_of(jobs["chart"])
        install = index_where(
            steps,
            lambda s: uses_of(s).startswith("sigstore/cosign-installer@"),
            "installing cosign",
        )
        assert SHA_PIN.search(uses_of(steps[install]))

        verify = index_where(
            steps,
            lambda s: re.search(r"\bcosign verify(?!-)", run_of(s)) is not None,
            "running `cosign verify`",
        )
        verify_script = run_of(steps[verify])
        assert '--certificate-identity "https://github.com/${GITHUB_WORKFLOW_REF}"' in verify_script
        assert (
            "--certificate-oidc-issuer https://token.actions.githubusercontent.com"
            in verify_script
        )

        pin = index_where(
            steps,
            lambda s: "release/pin_chart_digests.py" in run_of(s),
            "running release/pin_chart_digests.py",
        )
        assert "--values charts/curie/values.yaml" in run_of(steps[pin])

        package = index_where(steps, named("Package chart"), "named Package chart")
        assert install < verify <= pin < package
        for index in (verify, pin, package):
            not_soft_failing(steps[index])

    def test_every_chart_image_is_verified_and_pinned(self, jobs):
        steps = steps_of(jobs["chart"])
        verify = index_where(
            steps,
            lambda s: re.search(r"\bcosign verify(?!-)", run_of(s)) is not None,
            "running `cosign verify`",
        )
        pin = index_where(
            steps,
            lambda s: "release/pin_chart_digests.py" in run_of(s),
            "running release/pin_chart_digests.py",
        )
        assert "--image" in run_of(steps[pin])
        text = "\n".join(run_of(s) for s in steps[verify : pin + 1])
        for name in CHART_IMAGES:
            assert re.search(rf"(?<![\w-]){re.escape(name)}(?![\w-])", text), (
                f"{name} is not verified and pinned by the chart job"
            )


def test_stale_open_issue_comment_is_gone():
    # Issue 62 itself, not #628 or #629, which this file cites on purpose.
    assert not re.search(r"#62(?!\d)", WORKFLOW_PATH.read_text(encoding="utf-8"))
