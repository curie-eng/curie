"""The local-release image set is derived from compose profiles, not a growing list (#2245)."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "compose" / "ensure_release_images.py"
GENERATE = REPO_ROOT / "compose" / "generate_release_compose.py"
CI_YAML = REPO_ROOT / ".github" / "workflows" / "ci.yaml"
NIGHTLY_YAML = REPO_ROOT / ".github" / "workflows" / "nightly-graded-ladder.yaml"
LADDER = REPO_ROOT / "cli" / "scripts" / "e2e-ladder.sh"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


ensure = load_module(SCRIPT, "ensure_release_images")
generate = load_module(GENERATE, "generate_release_compose")


def test_oidc_stack_uses_the_candidate_gateway_on_its_private_network(tmp_path: Path) -> None:
    driver = load_module(REPO_ROOT / "tools/oidc-e2e/oidc_e2e.py", "oidc_e2e_image_check")
    settings = driver.Settings(
        project="acme-oidc",
        api_port=28400,
        ui_port=28480,
        dex_port=25556,
        image_tag="acme-candidate",
        ui_image="ghcr.io/curie-eng/curie-ui:acme-candidate",
        curie_bin=None,
        workdir=tmp_path,
    )
    env = driver.Stack(settings)._env(driver.oidc_on(settings))
    env["CURIE_LOCAL_GATEWAY_PORT"] = "38081"
    config = subprocess.run(
        [
            "docker", "compose", "--profile", "core",
            "-f", str(REPO_ROOT / "compose.dev.yaml"),
            "-f", str(driver.COMPOSE_OVERLAY), "config", "--format", "json",
        ],
        env={**os.environ, **env},
        text=True, capture_output=True, check=True,
    )
    services = json.loads(config.stdout)["services"]
    for name in ("curie-channel-gateway", "curie-gateway-replay-perms"):
        assert services[name]["image"] == "ghcr.io/curie-eng/curie-dispatcher:acme-candidate"
    gateway = services["curie-channel-gateway"]
    assert not gateway.get("network_mode")
    assert set(gateway["networks"]) == {"default"}
    assert not gateway.get("ports")
    assert gateway["environment"]["CURIE_CHANNEL_GATEWAY_PORT"] == "8081"
    assert services["curie-api"]["environment"]["CURIE_CHANNEL_GATEWAY_URL"] == (
        "http://curie-channel-gateway:8081"
    )


def generated_compose() -> str:
    return generate.generate(
        (REPO_ROOT / "compose.dev.yaml").read_text(),
        (REPO_ROOT / "otel" / "collector-config.yaml").read_text(),
        "latest",
    )


class TestImageListIsDerivedFromCompose:
    def test_full_plus_slack_includes_dispatcher_and_ui(self) -> None:
        images = ensure.curie_images_for_profiles(
            generated_compose(), profiles=("full", "slack")
        )
        names = {ensure.image_short_name(image) for image in images}
        assert "curie-dispatcher" in names
        assert "curie-ui" in names
        assert "curie-api" in names
        assert "curie-worker-local" in names

    @pytest.mark.parametrize("profile", ["core", "full"])
    def test_base_profile_requires_gateway_image_without_slack(self, profile: str) -> None:
        compose = generated_compose()
        images = ensure.curie_images_for_profiles(compose, profiles=(profile,))
        names = {ensure.image_short_name(image) for image in images}
        assert "curie-dispatcher" in names
        assert ("curie-ui" in names) == (profile == "full")
        services = yaml.safe_load(compose)["services"]
        assert profile in services["curie-channel-gateway"]["profiles"]
        assert services["curie-dispatcher"]["profiles"] == ["slack"]

    def test_every_listed_image_has_a_build_spec(self) -> None:
        images = ensure.required_release_images(
            generated_compose(), profiles=("full", "slack")
        )
        missing = [
            image
            for image in images
            if ensure.image_short_name(image) not in ensure.IMAGE_BUILDS
        ]
        assert missing == [], (
            "compose.release.yaml named curie images with no build mapping: "
            f"{missing}. Add them to IMAGE_BUILDS rather than a workflow step."
        )

    def test_required_set_includes_dispatcher_and_runner_even_on_full_only(self) -> None:
        images = ensure.required_release_images(
            generated_compose(), profiles=("full",)
        )
        names = {ensure.image_short_name(image) for image in images}
        assert "curie-dispatcher" in names
        assert "curie-runner" in names
        assert "curie-ui" in names

    def test_required_set_uses_resolved_dispatcher_and_runner_overrides(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        resolved = {
            "services": {
                "curie-api": {
                    "image": "ghcr.io/curie-eng/curie-api:overlay",
                    "profiles": ["core", "full"],
                },
                "curie-worker": {
                    "image": "ghcr.io/curie-eng/curie-worker-local:overlay",
                    "profiles": ["core", "full"],
                    "environment": {
                        "CURIE_RUNNER_IMAGE": (
                            "ghcr.io/curie-eng/curie-runner:overlay"
                        )
                    },
                },
                "curie-dispatcher": {
                    "image": "ghcr.io/curie-eng/curie-dispatcher:overlay",
                    "profiles": ["slack"],
                },
            }
        }
        compose_path = tmp_path / "compose.release.resolved.yaml"
        compose_path.write_text(yaml.safe_dump(resolved))

        def fake_run(cmd, **kwargs):
            if cmd[-2:] == ["config", "--images"]:
                stdout = "\n".join(
                    (
                        "ghcr.io/curie-eng/curie-api:overlay",
                        "ghcr.io/curie-eng/curie-worker-local:overlay",
                    )
                )
            elif "config" in cmd and "json" in cmd:
                stdout = json.dumps(resolved)
            else:
                stdout = yaml.safe_dump(resolved)
            return subprocess.CompletedProcess(cmd, 0, stdout=stdout, stderr="")

        monkeypatch.setattr(ensure.subprocess, "run", fake_run)
        images = ensure.required_release_images_from_file(
            compose_path, profiles=("core",)
        )

        assert set(images) == {
            "ghcr.io/curie-eng/curie-api:overlay",
            "ghcr.io/curie-eng/curie-worker-local:overlay",
            "ghcr.io/curie-eng/curie-dispatcher:overlay",
            "ghcr.io/curie-eng/curie-runner:overlay",
        }
        assert "ghcr.io/curie-eng/curie-dispatcher:latest" not in images
        assert "ghcr.io/curie-eng/curie-runner:latest" not in images

    @pytest.mark.parametrize(
        ("missing", "expected_field"),
        (
            ("dispatcher", "services.curie-dispatcher.image"),
            ("runner", "services.curie-worker.environment.CURIE_RUNNER_IMAGE"),
        ),
    )
    def test_required_set_refuses_a_missing_one_shot_image_identity(
        self, missing: str, expected_field: str
    ) -> None:
        resolved = {
            "services": {
                "curie-worker": {
                    "image": "ghcr.io/curie-eng/curie-worker-local:overlay",
                    "profiles": ["core", "full"],
                    "environment": {
                        "CURIE_RUNNER_IMAGE": (
                            "ghcr.io/curie-eng/curie-runner:overlay"
                        )
                    },
                },
                "curie-dispatcher": {
                    "image": "ghcr.io/curie-eng/curie-dispatcher:overlay",
                    "profiles": ["slack"],
                },
            }
        }
        if missing == "dispatcher":
            del resolved["services"]["curie-dispatcher"]["image"]
        else:
            environment = resolved["services"]["curie-worker"]["environment"]
            environment.pop("CURIE_RUNNER_IMAGE")

        with pytest.raises(SystemExit) as exc:
            ensure.required_release_images(
                yaml.safe_dump(resolved), profiles=("core",)
            )

        assert expected_field in str(exc.value)


class TestWorkflowsInvokeTheHelper:
    def test_nightly_local_release_builds_missing_images_from_the_helper(self) -> None:
        source = NIGHTLY_YAML.read_text()
        assert "compose/ensure_release_images.py" in source
        job = yaml.load(source, Loader=yaml.BaseLoader)["jobs"]["ladder-local-release"]
        runs = "\n".join(step.get("run", "") for step in job["steps"])
        assert "--build-missing" in runs
        assert "--profiles" in runs

    def test_ci_local_release_asserts_every_image_from_the_same_helper(self) -> None:
        # The CI rung loads images the ci-images job built once for the run, so
        # it must not rebuild any; the helper only proves each one is present.
        source = CI_YAML.read_text()
        assert "compose/ensure_release_images.py" in source
        job = yaml.load(source, Loader=yaml.BaseLoader)["jobs"]["e2e-ladder-release"]
        runs = "\n".join(step.get("run", "") for step in job["steps"])
        assert "compose/ensure_release_images.py --profiles full" in runs
        assert "--build-missing" not in runs
        assert "xargs docker image inspect" in runs

    def test_ladder_preflight_calls_the_helper_instead_of_hardcoding_slack(self) -> None:
        text = LADDER.read_text()
        start = text.index("rung_local_release() {")
        body = text[start : start + 4000]
        assert "ensure_release_images.py" in body
        assert "--profile slack config --images" not in body


def test_every_image_build_ignore_file_excludes_projects() -> None:
    # Security pin for #4562: root-context builds run COPY . . in their
    # builder stages, so a per-Dockerfile ignore file without **/.projects
    # would bake private worktree fixtures into builder layers.
    missing = []
    for name, build in ensure.IMAGE_BUILDS.items():
        ignore = REPO_ROOT / (build.dockerfile + ".dockerignore")
        patterns = [
            line.strip()
            for line in ignore.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        if "**/.projects" not in patterns:
            missing.append(f"{name}: {ignore.relative_to(REPO_ROOT)}")
    assert missing == [], (
        "image build ignore files must exclude **/.projects so private "
        f"worktree fixtures never enter builder layers: {missing}"
    )
