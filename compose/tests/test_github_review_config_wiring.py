"""Compose and release-generator wiring for GitHub review ingress (#2275)."""

import importlib.util
import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEV_PATH = REPO_ROOT / "compose.dev.yaml"
OTEL_PATH = REPO_ROOT / "otel" / "collector-config.yaml"
GENERATOR_PATH = REPO_ROOT / "compose" / "generate_release_compose.py"
FLAG = "GITHUB_REVIEW_INGRESS_ENABLED"
INTERVAL = "GITHUB_REVIEW_RECONCILER_INTERVAL_S"
EXPECTED_RAW = {
    FLAG: "${GITHUB_REVIEW_INGRESS_ENABLED:-false}",
    INTERVAL: "${GITHUB_REVIEW_RECONCILER_INTERVAL_S:-5}",
}


def _generate_release() -> str:
    spec = importlib.util.spec_from_file_location("github_review_release_generator", GENERATOR_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.generate(DEV_PATH.read_text(), OTEL_PATH.read_text(), version="9.9.9")


def _environment(service: dict[str, Any]) -> dict[str, str]:
    environment = service.get("environment", {})
    assert isinstance(environment, dict), "curie-api must use Compose environment map form"
    return {name: "" if value is None else str(value) for name, value in environment.items()}


def _documents() -> list[tuple[str, dict[str, Any]]]:
    return [
        ("compose.dev.yaml", yaml.safe_load(DEV_PATH.read_text())),
        ("generated compose.release.yaml", yaml.safe_load(_generate_release())),
    ]


def _resolved_environment(
    tmp_path: Path,
    label: str,
    raw_environment: dict[str, str],
    overrides: dict[str, str],
) -> dict[str, str]:
    compose_file = tmp_path / f"{label.replace(' ', '-').replace('.', '-')}.yaml"
    compose_file.write_text(
        yaml.safe_dump(
            {"services": {"api": {"image": "example-api", "environment": raw_environment}}}
        )
    )
    clean_environment = {
        name: value for name, value in os.environ.items() if name not in (FLAG, INTERVAL)
    }
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            "/dev/null",
            "-f",
            str(compose_file),
            "config",
            "--format",
            "json",
        ],
        env={**clean_environment, **overrides},
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)["services"]["api"]["environment"]


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, {FLAG: "false", INTERVAL: "5"}),
        ({FLAG: "true", INTERVAL: "2.5"}, {FLAG: "true", INTERVAL: "2.5"}),
    ],
)
def test_review_config_survives_release_generation_and_compose_interpolation(
    tmp_path: Path, overrides: dict[str, str], expected: dict[str, str]
) -> None:
    for label, document in _documents():
        api_environment = _environment(document["services"]["curie-api"])
        raw_review_environment = {name: api_environment[name] for name in EXPECTED_RAW}
        assert raw_review_environment == EXPECTED_RAW, f"{label} changed the review env contract"

        resolved = _resolved_environment(tmp_path, label, raw_review_environment, overrides)
        assert {name: resolved[name] for name in expected} == expected, label
