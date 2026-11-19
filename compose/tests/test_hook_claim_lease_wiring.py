"""Compose wiring for the shared hook claim lease.

curie-api and curie-worker read one delivery budget. The lease token is the
same on both services. A null mapping value passes the host value through.
An empty default would hand a default local up an empty lease.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEV_PATH = REPO_ROOT / "compose.dev.yaml"
OTEL_PATH = REPO_ROOT / "otel" / "collector-config.yaml"
GENERATOR_PATH = REPO_ROOT / "compose" / "generate_release_compose.py"
BUDGET = "CURIE_DELIVERY_BUDGET_S"
LEASE = "CURIE_HOOK_CLAIM_LEASE_S"
EXPECTED_BUDGET = "${CURIE_DELIVERY_BUDGET_S:-600}"
EMPTY_LEASE_DEFAULT = re.compile(r"^\$\{CURIE_HOOK_CLAIM_LEASE_S:-?\}$")
SERVICES = ("curie-api", "curie-worker")


def _generate_release() -> str:
    spec = importlib.util.spec_from_file_location(
        "hook_claim_lease_release_generator", GENERATOR_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.generate(DEV_PATH.read_text(), OTEL_PATH.read_text(), version="9.9.9")


def _environment(service: dict[str, Any]) -> dict[str, Any]:
    """Normalize Compose map or list environment form without rewriting null."""
    environment = service.get("environment") or {}
    if isinstance(environment, list):
        out: dict[str, Any] = {}
        for entry in environment:
            name, separator, value = str(entry).partition("=")
            # A bare list entry is the list form of a null mapping value.
            out[name] = value if separator else None
        return out
    return dict(environment)


def _documents() -> list[tuple[str, dict[str, Any]]]:
    return [
        ("compose.dev.yaml", yaml.safe_load(DEV_PATH.read_text())),
        ("generated compose.release.yaml", yaml.safe_load(_generate_release())),
    ]


def _token(label: str, service: str, environment: dict[str, Any], name: str) -> Any:
    assert name in environment, f"{label}: {service} does not carry {name}"
    return environment[name]


@pytest.mark.parametrize("label_and_document", _documents(), ids=lambda item: item[0])
def test_api_and_worker_share_the_delivery_budget_default(
    label_and_document: tuple[str, dict[str, Any]],
) -> None:
    label, document = label_and_document
    raw = {
        service: _token(label, service, _environment(document["services"][service]), BUDGET)
        for service in SERVICES
    }
    assert raw["curie-api"] == raw["curie-worker"] == EXPECTED_BUDGET, label


def test_lease_token_matches_and_is_not_an_empty_default() -> None:
    tokens: list[tuple[str, str, Any]] = []
    for label, document in _documents():
        for service in SERVICES:
            environment = _environment(document["services"][service])
            tokens.append((label, service, _token(label, service, environment, LEASE)))
    values = {token for _, _, token in tokens}
    assert len(values) == 1, tokens
    token = values.pop()
    assert not (isinstance(token, str) and EMPTY_LEASE_DEFAULT.fullmatch(token.strip())), token


def _resolved(tmp_path: Path, label: str, raw: str, overrides: dict[str, str]) -> dict[str, str]:
    compose_file = tmp_path / f"{label.replace(' ', '-').replace('.', '-')}.yaml"
    compose_file.write_text(
        yaml.safe_dump(
            {
                "services": {
                    service.replace("curie-", ""): {
                        "image": f"example-{service}",
                        "environment": {BUDGET: raw},
                    }
                    for service in SERVICES
                }
            }
        )
    )
    clean_environment = {name: value for name, value in os.environ.items() if name != BUDGET}
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
    return {name: services[name]["environment"][BUDGET] for name in ("api", "worker")}


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, "600"),
        ({BUDGET: ""}, "600"),
        ({BUDGET: "1800"}, "1800"),
    ],
    ids=["unset", "empty", "override"],
)
def test_delivery_budget_interpolation_agrees_on_both_services(
    tmp_path: Path, overrides: dict[str, str], expected: str
) -> None:
    for label, document in _documents():
        raw_values = [
            _token(label, service, _environment(document["services"][service]), BUDGET)
            for service in SERVICES
        ]
        assert raw_values[0] == raw_values[1] == EXPECTED_BUDGET, label
        resolved = _resolved(tmp_path, label, str(raw_values[0]), overrides)
        assert resolved == {"api": expected, "worker": expected}, label
