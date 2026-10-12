"""The deferred repository pool input has one pure model and exported schema."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from curie_worker.repo_runner_pools import RepoRunnerPools, build_schema, render_schema
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as SchemaValidationError
from pydantic import ValidationError

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schema" / "repo-runner-pools.schema.json"
INDEX, AMD64, ARM64, BASE = ("sha256:" + c * 64 for c in "abcd")


def _entry(**changes: Any) -> dict[str, Any]:
    return {
        "pool": "acme-repo-runner",
        "digests": [INDEX, AMD64, ARM64],
        "base": BASE,
        "docker": False,
        **changes,
    }


def test_committed_schema_matches_export() -> None:
    committed = SCHEMA_PATH.read_text(encoding="utf-8")
    assert render_schema() == committed, "repository pool JSON Schema must be regenerated"
    assert json.loads(committed) == build_schema()
    Draft202012Validator.check_schema(build_schema())


@pytest.mark.parametrize("path", ["acme-corp/api", "acme/platform/nested/api"])
@pytest.mark.parametrize("docker", [False, True])
def test_valid_repository_pool_loads(path: str, docker: bool) -> None:
    document = {
        path: _entry(
            docker=docker,
            egress=[
                {"cidr": "203.0.113.0/24", "ports": [{"protocol": "TCP", "port": 443}]},
            ],
        )
    }
    assert RepoRunnerPools.model_validate(document).model_dump(mode="json") == document
    Draft202012Validator(build_schema()).validate(document)


def test_empty_map_and_entry_without_egress_load() -> None:
    assert RepoRunnerPools.model_validate({}).root == {}
    document = {"acme-corp/api": _entry()}
    assert RepoRunnerPools.model_validate(document).root["acme-corp/api"].pool == "acme-repo-runner"
    Draft202012Validator(build_schema()).validate({})
    Draft202012Validator(build_schema()).validate(document)


@pytest.mark.parametrize(
    "changes",
    [
        {"pool": "Acme_Runner"},
        {"pool": "-acme"},
        {"pool": "a" * 64},
        {"digests": ["registry.example.com/acme/runner@" + INDEX, AMD64, ARM64]},
        {"digests": [INDEX, "sha256:" + "b" * 63, ARM64]},
        {"digests": [INDEX, AMD64]},
        {"digests": [INDEX, AMD64, ARM64, BASE]},
        {"base": "registry.example.com/acme/base:latest"},
        {"docker": "false"},
        {"docker": 0},
        {"egress": [{"cidr": "203.0.113.0/24", "ports": [{"protocol": "UDP", "port": 443}]}]},
        {"egress": [{"cidr": "203.0.113.0/24", "ports": [{"protocol": "TCP", "port": 0}]}]},
    ],
)
def test_model_and_schema_refuse_invalid_entries(changes: dict[str, Any]) -> None:
    document = {"acme-corp/api": _entry(**changes)}
    with pytest.raises(ValidationError):
        RepoRunnerPools.model_validate(document)
    with pytest.raises(SchemaValidationError):
        Draft202012Validator(build_schema()).validate(document)


@pytest.mark.parametrize("field", ["pool", "digests", "base", "docker"])
def test_model_and_schema_require_identity_fields(field: str) -> None:
    entry = _entry()
    del entry[field]
    document = {"acme-corp/api": entry}
    with pytest.raises(ValidationError):
        RepoRunnerPools.model_validate(document)
    with pytest.raises(SchemaValidationError):
        Draft202012Validator(build_schema()).validate(document)


@pytest.mark.parametrize("path", ["", "api", "/acme/api", "acme//api", "acme/api/"])
def test_model_and_schema_refuse_invalid_paths(path: str) -> None:
    document = {path: _entry()}
    with pytest.raises(ValidationError):
        RepoRunnerPools.model_validate(document)
    with pytest.raises(SchemaValidationError):
        Draft202012Validator(build_schema()).validate(document)


def test_model_refuses_invalid_cidr() -> None:
    entry = _entry(egress=[{"cidr": "invalid", "ports": [{"protocol": "TCP", "port": 443}]}])
    with pytest.raises(ValidationError):
        RepoRunnerPools.model_validate({"acme-corp/api": entry})


def test_model_refuses_malformed_json() -> None:
    with pytest.raises(ValidationError) as error:
        RepoRunnerPools.model_validate_json('{"acme-corp/api":')
    assert error.value.errors(include_input=False)[0]["type"] == "json_invalid"
