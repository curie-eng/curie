"""Compose and release-generator wiring for the action executor switch.

ACTION-EXECUTOR-1: one compose value renders ``CURIE_ACTION_EXECUTOR_ENABLED``
into both ``curie-api`` and ``curie-worker``, default off. Both the dev file and
the generated release compose must carry the same raw interpolation to both
services, so a single ``CURIE_ACTION_EXECUTOR_ENABLED=true`` in the operator's
environment turns both on and an unset one leaves both off.
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
FLAG = "CURIE_ACTION_EXECUTOR_ENABLED"
EXPECTED_RAW = "${CURIE_ACTION_EXECUTOR_ENABLED:-false}"
SERVICES = ("curie-api", "curie-worker")


def _generate_release() -> str:
    spec = importlib.util.spec_from_file_location(
        "action_executor_release_generator", GENERATOR_PATH
    )
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
def test_api_and_worker_carry_the_same_executor_switch(
    label_and_document: tuple[str, dict[str, Any]],
) -> None:
    label, document = label_and_document
    raw = {}
    for service in SERVICES:
        environment = _environment(document["services"][service])
        assert FLAG in environment, f"{label}: {service} does not carry {FLAG}"
        raw[service] = environment[FLAG]
    assert raw["curie-api"] == raw["curie-worker"], f"{label}: api and worker disagree: {raw!r}"
    assert raw["curie-api"] == EXPECTED_RAW, f"{label}: {FLAG} is {raw['curie-api']!r}"


def _resolved(tmp_path: Path, label: str, raw: str, overrides: dict[str, str]) -> dict[str, str]:
    compose_file = tmp_path / f"{label.replace(' ', '-').replace('.', '-')}.yaml"
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
    [({}, "false"), ({FLAG: "true"}, "true")],
    ids=["default-off", "one-value-on"],
)
def test_executor_switch_survives_release_generation_and_interpolation(
    tmp_path: Path, overrides: dict[str, str], expected: str
) -> None:
    for label, document in _documents():
        api_raw = _environment(document["services"]["curie-api"]).get(FLAG)
        worker_raw = _environment(document["services"]["curie-worker"]).get(FLAG)
        assert api_raw is not None and worker_raw is not None, f"{label}: {FLAG} missing"
        for raw in (api_raw, worker_raw):
            resolved = _resolved(tmp_path, label, raw, overrides)
            assert resolved == {"api": expected, "worker": expected}, label
