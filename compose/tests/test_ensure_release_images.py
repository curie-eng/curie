"""The local-release image set is derived from compose profiles, not a growing list (#2245)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

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

    def test_full_without_slack_does_not_require_dispatcher(self) -> None:
        images = ensure.curie_images_for_profiles(
            generated_compose(), profiles=("full",)
        )
        names = {ensure.image_short_name(image) for image in images}
        assert "curie-dispatcher" not in names
        assert "curie-ui" in names

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


class TestWorkflowsInvokeTheHelper:
    def test_nightly_local_release_builds_missing_images_from_the_helper(self) -> None:
        source = NIGHTLY_YAML.read_text()
        assert "compose/ensure_release_images.py" in source
        job = yaml.load(source, Loader=yaml.BaseLoader)["jobs"]["ladder-local-release"]
        runs = "\n".join(step.get("run", "") for step in job["steps"])
        assert "--build-missing" in runs
        assert "--profiles" in runs

    def test_ci_local_release_builds_missing_images_from_the_same_helper(self) -> None:
        source = CI_YAML.read_text()
        assert "compose/ensure_release_images.py" in source
        job = yaml.load(source, Loader=yaml.BaseLoader)["jobs"]["e2e-ladder-release"]
        runs = "\n".join(step.get("run", "") for step in job["steps"])
        assert "--build-missing" in runs

    def test_ladder_preflight_calls_the_helper_instead_of_hardcoding_slack(self) -> None:
        text = LADDER.read_text()
        start = text.index("rung_local_release() {")
        body = text[start : start + 4000]
        assert "ensure_release_images.py" in body
        assert "--profile slack config --images" not in body
