"""#2424: local-release image set is derived from the generated compose artifact.

The local-release rung and its CI/nightly jobs must provision every
ghcr.io/curie-eng/curie-* image the generated compose.release.yaml requires
for the profiles it actually starts (plus slack, because local message runs a
one-shot dispatcher). Adding images one at a time is how #2005 closed and the
next night failed on dispatcher.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
GENERATE_SCRIPT = REPO_ROOT / "compose" / "generate_release_compose.py"
RELEASE_IMAGES_SCRIPT = REPO_ROOT / "compose" / "release_images.py"
DEV_PATH = REPO_ROOT / "compose.dev.yaml"
OTEL_PATH = REPO_ROOT / "otel" / "collector-config.yaml"
NIGHTLY_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "nightly-graded-ladder.yaml"
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yaml"
LADDER_SCRIPT = REPO_ROOT / "cli" / "scripts" / "e2e-ladder.sh"


def load_release_images():
    spec = importlib.util.spec_from_file_location("release_images", RELEASE_IMAGES_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_generate():
    spec = importlib.util.spec_from_file_location("generate_release_compose", GENERATE_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.generate


def generated_compose(version: str = "latest") -> str:
    return load_generate()(DEV_PATH.read_text(), OTEL_PATH.read_text(), version=version)


def write_compose(tmp_path: Path, text: str | None = None) -> Path:
    path = tmp_path / "compose.release.yaml"
    path.write_text(text if text is not None else generated_compose())
    return path


def run_release_images(
    *args: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    merged = os.environ.copy()
    if env:
        merged.update(env)
    return subprocess.run(
        [sys.executable, str(RELEASE_IMAGES_SCRIPT), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=merged,
    )


def job_by_name(workflow_path: Path, job_name: str) -> dict:
    workflow = yaml.safe_load(workflow_path.read_text())
    job = workflow["jobs"][job_name]
    assert isinstance(job, dict)
    return job


def test_runtime_module_does_not_import_yaml_at_load() -> None:
    for line in RELEASE_IMAGES_SCRIPT.read_text().splitlines():
        if line.startswith("import yaml") or line.startswith("from yaml"):
            raise AssertionError(
                "compose/release_images.py must lazy-import yaml; "
                "the local-release ladder jobs do not install PyYAML"
            )


def test_required_from_artifact_falls_back_to_compose_cli(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if shutil.which("docker") is None:
        pytest.skip("docker is required for compose config --images")
    images = load_release_images()
    monkeypatch.setattr(images, "_yaml", lambda: None)
    compose_path = write_compose(tmp_path)
    try:
        derived = images.required_from_artifact(compose_path, {"full", "slack"})
    except SystemExit as exc:
        pytest.skip(f"docker compose config failed: {exc}")
    assert "ghcr.io/curie-eng/curie-dispatcher:latest" in derived
    assert "ghcr.io/curie-eng/curie-ui:latest" in derived


def test_full_slack_derives_the_generated_compose_curie_images() -> None:
    images = load_release_images()
    required = images.required_curie_images(generated_compose(), {"full", "slack"})
    assert required == {
        "ghcr.io/curie-eng/curie-api:latest",
        "ghcr.io/curie-eng/curie-dispatcher:latest",
        "ghcr.io/curie-eng/curie-ui:latest",
        "ghcr.io/curie-eng/curie-worker-local:latest",
    }


def test_core_profile_omits_ui_and_keeps_dispatcher_via_slack() -> None:
    images = load_release_images()
    required = images.required_curie_images(generated_compose(), {"core", "slack"})
    assert "ghcr.io/curie-eng/curie-ui:latest" not in required
    assert "ghcr.io/curie-eng/curie-dispatcher:latest" in required
    assert "ghcr.io/curie-eng/curie-api:latest" in required
    assert "ghcr.io/curie-eng/curie-worker-local:latest" in required


def test_every_derived_image_has_a_local_build_recipe() -> None:
    images = load_release_images()
    required = images.required_curie_images(generated_compose(), {"full", "slack"})
    missing = [ref for ref in sorted(required) if images.recipe_for(ref) is None]
    assert missing == [], f"generated compose requires images with no local recipe: {missing}"


@pytest.mark.skipif(
    shutil.which("docker") is None, reason="docker is required for compose config --images"
)
def test_derivation_matches_docker_compose_config_images(tmp_path: Path) -> None:
    compose_path = write_compose(tmp_path)
    rendered = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(compose_path),
            "--profile",
            "full",
            "--profile",
            "slack",
            "config",
            "--images",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if rendered.returncode != 0:
        pytest.skip(f"docker compose config failed: {rendered.stderr.strip()}")
    compose_set = {
        line.strip()
        for line in rendered.stdout.splitlines()
        if line.strip().startswith("ghcr.io/curie-eng/curie-")
    }
    images = load_release_images()
    derived = images.required_curie_images(compose_path.read_text(), {"full", "slack"})
    assert derived == compose_set


def test_check_reports_a_missing_identity_and_recovery_without_pulling(
    tmp_path: Path,
) -> None:
    compose_path = write_compose(tmp_path)
    missing = "ghcr.io/curie-eng/curie-dispatcher:latest"
    docker_log = tmp_path / "docker.log"
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    stub = stub_dir / "docker"
    stub.write_text(
        "\n".join(
            [
                "#!/bin/sh",
                f'echo "$@" >> "{docker_log}"',
                'if [ "$1" = "pull" ]; then',
                '  echo "unexpected docker pull $*" >&2',
                "  exit 99",
                "fi",
                'if [ "$1" = "image" ] && [ "$2" = "inspect" ]; then',
                '  image="$3"',
                '  [ "$image" = "--format" ] && image="$5"',
                f'  if [ "$image" = "{missing}" ]; then',
                "    exit 1",
                "  fi",
                "  exit 0",
                "fi",
                "exit 0",
                "",
            ]
        )
    )
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)

    env = {
        "PATH": f"{stub_dir}:{os.environ.get('PATH', '')}",
        "CURIE_RELEASE_IMAGES_DOCKER": str(stub),
    }
    result = run_release_images(
        "--compose",
        str(compose_path),
        "--profiles",
        "full,slack",
        "--check",
        env=env,
    )
    assert result.returncode != 0, result.stdout + result.stderr
    combined = result.stdout + result.stderr
    assert missing in combined
    assert "is required by compose.release.yaml" in combined
    assert "fix:" in combined.lower()
    log = docker_log.read_text() if docker_log.exists() else ""
    assert "pull" not in log.split()
    assert "pull" not in combined.lower()


def test_unknown_recipe_fails_with_identity_even_when_the_image_is_present(
    tmp_path: Path,
) -> None:
    extra = "ghcr.io/curie-eng/curie-missing:latest"
    doc = yaml.safe_load(generated_compose())
    doc["services"]["curie-missing"] = {
        "image": extra,
        "profiles": ["full"],
    }
    compose_path = write_compose(tmp_path, yaml.safe_dump(doc))
    result = run_release_images(
        "--compose",
        str(compose_path),
        "--profiles",
        "full,slack",
        "--check",
    )
    assert result.returncode != 0, result.stdout + result.stderr
    combined = result.stdout + result.stderr
    assert extra in combined
    assert "no local build recipe" in combined
    assert "fix:" in combined.lower()


def test_job_tags_ignore_pull_and_inspect() -> None:
    images = load_release_images()
    pulled = {
        "steps": [
            {
                "run": "docker pull ghcr.io/curie-eng/curie-dispatcher:latest\n"
                "docker image inspect ghcr.io/curie-eng/curie-ui:latest"
            }
        ]
    }
    assert images.job_curie_image_tags(pulled) == set()
    tagged = {
        "steps": [
            {
                "uses": "docker/build-push-action@v7",
                "with": {"tags": "ghcr.io/curie-eng/curie-dispatcher:latest"},
            },
            {
                "run": "docker tag ghcr.io/curie-eng/curie-api:ci-local "
                "ghcr.io/curie-eng/curie-api:latest\n"
                "docker build --build-arg BASE_TAG=ci-local "
                "-t ghcr.io/curie-eng/curie-worker-local:latest "
                "-f compose/worker-local.Dockerfile compose"
            },
        ]
    }
    assert images.job_curie_image_tags(tagged) == {
        "ghcr.io/curie-eng/curie-dispatcher:latest",
        "ghcr.io/curie-eng/curie-api:latest",
        "ghcr.io/curie-eng/curie-worker-local:latest",
    }


@pytest.mark.parametrize(
    ("workflow_path", "job_name"),
    [
        (NIGHTLY_WORKFLOW, "ladder-local-release"),
        (CI_WORKFLOW, "e2e-ladder-release"),
    ],
)
def test_local_release_job_tags_every_generated_compose_image(
    workflow_path: Path, job_name: str
) -> None:
    images = load_release_images()
    required = images.required_curie_images(generated_compose(), {"full", "slack"})
    provisioned = images.job_curie_image_tags(job_by_name(workflow_path, job_name))
    missing = sorted(required - provisioned)
    assert missing == [], (
        f"{workflow_path.name} job {job_name} does not tag the generated "
        f"compose.release.yaml identities {missing}; derive the set from the "
        "artifact instead of adding images one at a time"
    )


def test_ladder_local_release_rung_calls_the_shared_checker() -> None:
    text = LADDER_SCRIPT.read_text()
    start = text.index("rung_local_release() {")
    end = text.index('assert_bundle_identity "local-release"', start)
    body = text[start:end]
    assert "compose/release_images.py" in body
    assert "--check" in body
    assert "docker pull" not in body
    assert "curie-dispatcher:latest" not in body
