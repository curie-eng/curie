"""Compose and release-generator wiring for the executor sandbox cap.

@spec AUTOMATED-REMEDIATION-12 (executor amendment E9): one compose value
renders ``CURIE_ACTION_EXECUTOR_MAX_CONCURRENT_SANDBOXES`` into both
``curie-api`` (whose claim route enforces it) and ``curie-worker`` (whose
executor loop may run up to that many at once), default 2, in the dev file and
in the generated release compose, so a single value moves both and an unset one
leaves both at 2. This is the compose half of the chart's
``actionExecutor.maxConcurrentSandboxes`` (``charts/curie/ci/
executor-sandbox-cap-assertions.sh``).
"""

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
FLAG = "CURIE_ACTION_EXECUTOR_MAX_CONCURRENT_SANDBOXES"
EXPECTED_RAW = "${CURIE_ACTION_EXECUTOR_MAX_CONCURRENT_SANDBOXES:-2}"
SERVICES = ("curie-api", "curie-worker")


def _generate_release() -> str:
    spec = importlib.util.spec_from_file_location("sandbox_cap_release_generator", GENERATOR_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.generate(DEV_PATH.read_text(), OTEL_PATH.read_text(), version="9.9.9")


def _environment(service: dict[str, Any]) -> dict[str, str]:
    """Normalize Compose map or list ``environment`` form to a dict."""
    environment = service.get("environment") or {}
    if isinstance(environment, list):
        out: dict[str, str] = {}
        for entry in environment:
            name, _, value = str(entry).partition("=")
            out[name] = value
        return out
    return {name: "" if value is None else str(value) for name, value in environment.items()}


def _documents() -> list[tuple[str, dict[str, Any]]]:
    return [
        ("compose.dev.yaml", yaml.safe_load(DEV_PATH.read_text())),
        ("generated compose.release.yaml", yaml.safe_load(_generate_release())),
    ]


@pytest.mark.parametrize("label_and_document", _documents(), ids=lambda item: item[0])
def test_api_and_worker_carry_the_same_sandbox_cap(
    label_and_document: tuple[str, dict[str, Any]],
) -> None:
    """@spec AUTOMATED-REMEDIATION-12"""

    label, document = label_and_document
    raw = {}
    for service in SERVICES:
        environment = _environment(document["services"][service])
        assert FLAG in environment, f"{label}: {service} does not carry {FLAG}"
        raw[service] = environment[FLAG]
    assert raw["curie-api"] == raw["curie-worker"], f"{label}: api and worker disagree: {raw!r}"
    assert raw["curie-api"] == EXPECTED_RAW, f"{label}: {FLAG} is {raw['curie-api']!r}"


def _resolved(tmp_path: Path, raw: str, overrides: dict[str, str]) -> dict[str, str]:
    compose_file = tmp_path / "sandbox-cap.yaml"
    compose_file.write_text(
        yaml.safe_dump(
            {
                "services": {
                    service.replace("curie-", ""): {
                        "image": f"example-{service}",
                        "environment": {FLAG: raw},
                    }
                    for service in SERVICES
                }
            }
        )
    )
    clean_environment = {name: value for name, value in os.environ.items() if name != FLAG}
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
    services = json.loads(result.stdout)["services"]
    return {name: services[name]["environment"][FLAG] for name in ("api", "worker")}


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [({}, "2"), ({FLAG: "3"}, "3")],
    ids=["default-two", "one-value-moves-both"],
)
def test_the_sandbox_cap_survives_release_generation_and_interpolation(
    tmp_path: Path, overrides: dict[str, str], expected: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-12"""

    for label, document in _documents():
        for service in SERVICES:
            raw = _environment(document["services"][service]).get(FLAG)
            assert raw is not None, f"{label}: {service} does not carry {FLAG}"
            assert _resolved(tmp_path, raw, overrides) == {"api": expected, "worker": expected}
