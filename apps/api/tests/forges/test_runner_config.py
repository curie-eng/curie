"""Factory tool contracts are validated at the production config boundary."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
from curie_api.config import Settings
from curie_api.forges.config import ForgesConfig, RepositoryBindingConfig
from curie_api.forges.loader import load_forges_json
from curie_api.forges.runner_config import (
    CATALOG_TOOLS,
    DockerConfig,
    RunnerConfig,
    ToolCatalogEntry,
    ToolManifest,
    catalog_entry_schema,
    manifest_schema,
)
from forge_fakes import config_samples as samples
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as SchemaValidationError
from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[4]
INDEX, AMD64, ARM64, BASE = ("sha256:" + c * 64 for c in "abcd")


def _runner(**changes: Any) -> dict[str, Any]:
    return {
        "tools": ["uv", "rust", "pnpm", "build-essential", "docker-cli"],
        "image": "registry.example.com/acme/runner@" + INDEX,
        "base": BASE,
        "platform_digests": {"linux/amd64": AMD64, "linux/arm64": ARM64},
        "dockerfile": "Dockerfile.runner",
        "probes": [{"name": "custom-check", "command": ["custom-check", "--version"]}],
        **changes,
    }


def _egress() -> list[dict[str, Any]]:
    return [{"cidr": "203.0.113.0/24", "ports": [{"protocol": "TCP", "port": 443}]}]


def _file_document(tmp_path: Path, **blocks: Any) -> dict[str, Any]:
    document = samples.github_document()
    document["bindings"][0]["repos"][0].update(blocks)
    key = tmp_path / "app-key"
    key.write_text(samples.PLACEHOLDER_APP_KEY, encoding="utf-8")
    for section in ("code_hosts", "trackers"):
        for entry in document[section]:
            del entry["credential"]["private_key"]
            entry["credential"]["private_key_file"] = str(key)
    return document


def _settings(document: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.setenv("CURIE_FORGES", json.dumps(document))
    return Settings(_env_file=None, environment="dev")


def test_complete_runner_and_docker_load_through_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert set(CATALOG_TOOLS) == {"uv", "rust", "pnpm", "build-essential", "docker-cli"}
    doc = _file_document(
        tmp_path, runner=_runner(), docker={"enabled": True, "egress_allowlist": _egress()}
    )
    config = _settings(doc, monkeypatch).forges
    repo = config.bindings[0].repos[0]
    assert repo.runner is not None and repo.runner.model_dump(mode="json") == _runner()
    assert repo.docker is not None and repo.docker.enabled is True
    assert repo.docker.model_dump(mode="json")["egress_allowlist"] == _egress()
    assert config == load_forges_json(json.dumps(doc))


def test_chart_runner_tools_match_the_catalog_contract() -> None:
    schema = json.loads((ROOT / "charts/curie/values.schema.json").read_text())
    tools = schema["definitions"]["repositoryRunner"]["properties"]["tools"]["items"]["enum"]
    assert set(tools) == set(CATALOG_TOOLS)


@pytest.mark.parametrize(
    "changes",
    [
        {"image": "registry.example.com/acme/runner:latest"},
        {"image": "registry.example.com/acme/runner@sha256:" + "a" * 63},
        {"tools": ["unknown-tool"]},
        {"tools": ["uv", "uv"]},
        {"base": "registry.example.com/acme/base@" + BASE},
        {"platform_digests": {"linux/amd64": AMD64}},
        {"probes": [{"name": "check", "command": []}]},
        {"probes": [{"name": "check", "command": [" \t"]}]},
        {"probes": [{"name": "check", "command": ["x"]}] * 2},
    ],
)
def test_invalid_runner_refused_through_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, changes: dict[str, Any]
) -> None:
    with pytest.raises(ValidationError, match="runner"):
        _settings(_file_document(tmp_path, runner=_runner(**changes)), monkeypatch)


@pytest.mark.parametrize(
    "blocks",
    [
        {"runner": {"tools": ["uv"]}},
        {"docker": {"enabled": True}},
        {"runner": _runner(tools=["uv"]), "docker": {"enabled": True}},
    ],
)
def test_incomplete_runner_or_docker_dependency_refused_through_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, blocks: dict[str, Any]
) -> None:
    with pytest.raises(ValidationError, match="runner|docker"):
        _settings(_file_document(tmp_path, **blocks), monkeypatch)


@pytest.mark.parametrize("tag", ["", ":v1"])
def test_registry_port_and_optional_tag_accept_digest_pins(tag: str) -> None:
    image = "registry.example.com:5000/acme/runner" + tag + "@" + INDEX
    runner = _runner(image=image, probes=[{"name": "check", "command": ["check", "", "-v"]}])
    assert RunnerConfig.model_validate(runner).model_dump(mode="json") == runner


@pytest.mark.parametrize(
    "blocks",
    [
        {},
        {"docker": {"enabled": False}},
        {"docker": {"enabled": False, "egress_allowlist": _egress()}},
    ],
)
def test_legacy_and_disabled_docker_without_runner_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, blocks: dict[str, Any]
) -> None:
    repo = _settings(_file_document(tmp_path, **blocks), monkeypatch).forges.bindings[0].repos[0]
    assert repo.runner is None
    assert repo.docker is None or repo.docker.enabled is False
    assert RepositoryBindingConfig.model_validate(
        {**samples.github_binding()["repos"][0], **blocks}
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"cidr": "invalid"},
        {"ports": [{"protocol": "UDP", "port": 443}]},
        {"ports": [{"protocol": "TCP", "port": 0}]},
    ],
)
def test_invalid_docker_egress_refused(changes: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        DockerConfig.model_validate(
            {"enabled": False, "egress_allowlist": [{**_egress()[0], **changes}]}
        )


def _shared_document(case: str) -> dict[str, Any]:
    github = case.startswith("github")
    host = samples.github_code_host("forge") if github else samples.gitlab_code_host("forge")
    repo = {"alias": "api", "code_host": "forge", "project_id": "4242", "path": "acme-corp/api"}
    binding = samples.jira_binding(
        repos=[repo], default_repo="api", components={}, review_feedback_allowlist={}
    )
    second = copy.deepcopy(binding)
    second.update(name="acme-jira-second", tracker_scope_id="second-example-site")
    other = second["repos"][0]
    document = {
        "code_hosts": [host],
        "trackers": [samples.jira_tracker()],
        "bindings": [binding, second],
    }
    if case not in {"cross-host-legacy", "disabled-equivalent"}:
        repo["runner"] = other["runner"] = _runner()
    if case in {"runner-conflict", "github-conflict", "host-alias-conflict"}:
        other["runner"] = _runner(tools=["uv"])
    if case == "docker-conflict":
        repo["docker"], other["docker"] = {"enabled": True}, {"enabled": False}
    if case.startswith("github"):
        other["path"] = "Acme-Corp/API"
    if case.startswith("cross-host") or case == "host-alias-conflict":
        alias = copy.deepcopy(host)
        alias["name"] = "other-forge"
        if case != "host-alias-conflict":
            alias.update(
                host="other-gitlab.example.com", api_url="https://other-gitlab.example.com/api/v4"
            )
        document["code_hosts"].append(alias)
        other["code_host"] = "other-forge"
    if case == "disabled-equivalent":
        other["docker"] = {"enabled": False}
    return document


@pytest.mark.parametrize(
    ("case", "refused"),
    [
        ("runner-conflict", True),
        ("docker-conflict", True),
        ("identical", False),
        ("github-identical", False),
        ("github-conflict", True),
        ("cross-host-active", True),
        ("cross-host-legacy", False),
        ("disabled-equivalent", False),
        ("host-alias-conflict", True),
    ],
)
def test_repository_identity_and_path_consistency(case: str, refused: bool) -> None:
    document = _shared_document(case)
    if refused:
        with pytest.raises(ValidationError) as error:
            ForgesConfig.model_validate(document)
        if case == "cross-host-active":
            terms: tuple[str, ...] = ("path",)
        elif case == "host-alias-conflict":
            # Two entries for one kind and host are refused before any runner
            # comparison: a stored repository names one entry by kind and host.
            terms = ("one code host entry per host",)
        else:
            terms = ("runner", "docker")
        messages = [e["msg"].lower() for e in error.value.errors(include_input=False)]
        assert any(term in message for term in terms for message in messages), messages
    else:
        assert len(ForgesConfig.model_validate(document).bindings) == 2


def _manifest() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "docker_cli": False,
        "tools": [
            {"name": "uv", "version": "0.7", "path": "/bin/uv", "source": "catalog"},
            {"name": "check", "version": "1.0", "path": "/opt/bin/check", "source": "probe"},
        ],
    }


def _catalog() -> dict[str, Any]:
    return {
        "name": "uv",
        "platforms": ["linux/amd64", "linux/arm64"],
        "source_digests": {
            "linux/amd64": "registry.example.com/acme/uv@" + AMD64,
            "linux/arm64": "registry.example.com/acme/uv@" + ARM64,
        },
        "cache_env": {"UV_CACHE_DIR": "/home/runner/.cache/uv"},
        "registry_hosts": ["registry.example.com"],
        "probe": ["uv", "--version"],
    }


@pytest.mark.parametrize(
    ("name", "model", "schema", "document"),
    [
        ("tool-manifest", ToolManifest, manifest_schema, _manifest()),
        ("tool-catalog-entry", ToolCatalogEntry, catalog_entry_schema, _catalog()),
    ],
)
def test_committed_schema_and_valid_example(
    name: str, model: Any, schema: Any, document: dict[str, Any]
) -> None:
    exported = schema()
    assert json.loads((ROOT / "docs/schemas" / f"{name}.schema.json").read_text()) == exported
    Draft202012Validator.check_schema(exported)
    Draft202012Validator(exported).validate(document)
    assert model.model_validate(document).model_dump(mode="json") == document


@pytest.mark.parametrize(
    ("model", "schema", "document"),
    [
        (ToolManifest, manifest_schema, {**_manifest(), "schema_version": 2}),
        (ToolManifest, manifest_schema, {**_manifest(), "docker_cli": "false"}),
        (ToolCatalogEntry, catalog_entry_schema, {**_catalog(), "name": "unknown-tool"}),
        (ToolCatalogEntry, catalog_entry_schema, {**_catalog(), "platforms": ["windows/amd64"]}),
        (ToolCatalogEntry, catalog_entry_schema, {**_catalog(), "probe": []}),
        (ToolCatalogEntry, catalog_entry_schema, {**_catalog(), "probe": [" \t"]}),
        (
            ToolCatalogEntry,
            catalog_entry_schema,
            {
                **_catalog(),
                "source_digests": {"linux/amd64": "registry.example.com/acme/uv:latest"},
            },
        ),
    ],
)
def test_model_and_schema_refuse_malformed_examples(
    model: Any, schema: Any, document: dict[str, Any]
) -> None:
    with pytest.raises(ValidationError):
        model.model_validate(document)
    with pytest.raises(SchemaValidationError):
        Draft202012Validator(schema()).validate(document)
