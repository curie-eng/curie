"""Chart, schema and worker startup agree on the internal daemon handoff."""

from __future__ import annotations

import copy
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml
from curie_worker.repo_runner_pools import build_schema, parse_repo_runner_pools
from curie_worker.sandbox.daemon_config import ToolDaemonConfig
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as SchemaValidationError
from jsonschema.validators import validator_for
from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[4]
CHART = ROOT / "charts/curie"
VECTOR = json.loads((ROOT / "tests/vectors/daemon-config.json").read_text())
DAEMON = VECTOR["daemon"]
REPO = "acme-corp/api"
QUANTITY_PATHS = [
    ("resources", group, resource)
    for group in ("requests", "limits")
    for resource in ("cpu", "memory", "ephemeral-storage")
] + [("storageSizeLimit",)]
DAEMON_FIELDS = (
    "namespace",
    "image",
    "port",
    "selectorLabels",
    "resources",
    "storageSizeLimit",
    "maxClaimLifetimeSeconds",
    "pingTimeoutSeconds",
)


def _set(target: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value


def _document(daemon: Any) -> dict[str, Any]:
    return {
        REPO: {
            "pool": "acme-api-runner",
            "digests": ["sha256:" + c * 64 for c in "abc"],
            "base": "sha256:" + "d" * 64,
            "docker": True,
            "egress": [],
            "daemon": daemon,
        }
    }


def _assert_contract(daemon: Any, *, valid: bool) -> None:
    document = _document(daemon)
    schemas = (
        (Draft202012Validator(ToolDaemonConfig.model_json_schema()), daemon),
        (Draft202012Validator(build_schema()), document),
    )
    if valid:
        assert ToolDaemonConfig.model_validate(daemon).model_dump(mode="json") == daemon
        parsed = parse_repo_runner_pools(json.dumps(document))
        assert parsed.model_dump(mode="json") == document
        for validator, value in schemas:
            validator.validate(value)
    else:
        with pytest.raises(ValidationError):
            ToolDaemonConfig.model_validate(daemon)
        with pytest.raises(ValueError):
            parse_repo_runner_pools(json.dumps(document))
        for validator, value in schemas:
            with pytest.raises(SchemaValidationError):
                validator.validate(value)


def test_complete_daemon_has_exact_required_serialized_fields_and_is_frozen() -> None:
    assert set(DAEMON) == set(DAEMON_FIELDS)
    _assert_contract(copy.deepcopy(DAEMON), valid=True)
    config = ToolDaemonConfig.model_validate(DAEMON)
    with pytest.raises(ValidationError, match="frozen"):
        config.port = 2443
    with pytest.raises(ValidationError, match="frozen"):
        config.resources.requests.cpu = "1"
    assert config.model_dump(mode="json") == DAEMON
    Draft202012Validator.check_schema(ToolDaemonConfig.model_json_schema())


@pytest.mark.parametrize("field", DAEMON_FIELDS)
def test_every_daemon_field_is_required_without_worker_defaults(field: str) -> None:
    daemon = copy.deepcopy(DAEMON)
    del daemon[field]
    _assert_contract(daemon, valid=False)


@pytest.mark.parametrize(
    "path",
    [("resources", "requests"), ("resources", "limits")]
    + QUANTITY_PATHS[:-1]
    + [("selectorLabels", key) for key in DAEMON["selectorLabels"]],
)
def test_nested_resources_and_complete_selector_are_required(path: tuple[str, ...]) -> None:
    daemon = copy.deepcopy(DAEMON)
    parent = daemon
    for key in path[:-1]:
        parent = parent[key]
    del parent[path[-1]]
    _assert_contract(daemon, valid=False)


@pytest.mark.parametrize(
    "path",
    [(), ("resources",), ("resources", "requests"), ("resources", "limits"), ("selectorLabels",)],
)
def test_unknown_fields_are_refused_at_every_level(path: tuple[str, ...]) -> None:
    daemon = copy.deepcopy(DAEMON)
    target = daemon
    for key in path:
        target = target[key]
    target["unexpected"] = "1Gi"
    _assert_contract(daemon, valid=False)


@pytest.mark.parametrize("value", [None, False, [], "daemon"])
def test_daemon_must_be_an_object(value: Any) -> None:
    _assert_contract(value, valid=False)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("namespace",), ""),
        (("namespace",), "Acme"),
        (("namespace",), "acme_system"),
        (("namespace",), "a.b"),
        (("namespace",), "-acme"),
        (("namespace",), "acme-"),
        (("namespace",), "a" * 64),
        (("namespace",), "acme\n"),
        (("namespace",), 1),
        (("image",), ""),
        (("image",), "registry.example.com/acme/daemon:latest"),
        (("image",), "registry.example.com/acme/daemon@sha256:abc"),
        (("image",), "registry.example.com/acme/daemon@sha256:" + "A" * 64),
        (("image",), "@sha256:" + "a" * 64),
        (("image",), "bad image@sha256:" + "a" * 64),
        (("image",), "bad'image@sha256:" + "a" * 64),
        (("image",), 'bad"image@sha256:' + "a" * 64),
        (("image",), "bad\\image@sha256:" + "a" * 64),
        (("image",), DAEMON["image"] + "\n"),
        (("image",), False),
        (("port",), 0),
        (("port",), 65536),
        (("port",), "2376"),
        (("port",), True),
        (("port",), 1.5),
        (("port",), None),
        (("selectorLabels",), {}),
        (("selectorLabels",), []),
        (("selectorLabels", "app.kubernetes.io/name"), ""),
        (("selectorLabels", "app.kubernetes.io/name"), "a" * 64),
        (("selectorLabels", "app.kubernetes.io/instance"), "-acme"),
        (("selectorLabels", "app.kubernetes.io/instance"), "acme/other"),
        (("selectorLabels", "app.kubernetes.io/instance"), "acme\n"),
        (("selectorLabels", "app.kubernetes.io/component"), "runner-sandbox"),
        (("selectorLabels", "app.kubernetes.io/component"), ""),
        (("selectorLabels", "app.kubernetes.io/name"), True),
        (("resources",), None),
        (("resources",), []),
        (("resources", "requests"), False),
        (("resources", "limits"), "1Gi"),
    ],
)
def test_model_parser_and_schemas_refuse_invalid_daemon_fields(
    path: tuple[str, ...], value: Any
) -> None:
    daemon = copy.deepcopy(DAEMON)
    _set(daemon, path, value)
    _assert_contract(daemon, valid=False)


@pytest.mark.parametrize("field", ["maxClaimLifetimeSeconds", "pingTimeoutSeconds"])
@pytest.mark.parametrize("value", [0, -1, True, False, 1.5, "2", None, [], {}])
def test_timeouts_are_strict_positive_integers(field: str, value: Any) -> None:
    daemon = copy.deepcopy(DAEMON)
    daemon[field] = value
    _assert_contract(daemon, valid=False)


@pytest.mark.parametrize("field", ["port", "maxClaimLifetimeSeconds", "pingTimeoutSeconds"])
def test_worker_refuses_integral_float_coercion(field: str) -> None:
    daemon = copy.deepcopy(DAEMON)
    daemon[field] = 2.0
    with pytest.raises(ValidationError):
        ToolDaemonConfig.model_validate(daemon)
    with pytest.raises(ValueError):
        parse_repo_runner_pools(json.dumps(_document(daemon)))


@pytest.mark.parametrize("port", [1, 65535])
def test_valid_boundary_values_preserve_scalar_types(port: int) -> None:
    daemon = copy.deepcopy(DAEMON)
    daemon.update(namespace="a" * 63, port=port, maxClaimLifetimeSeconds=1, pingTimeoutSeconds=1)
    daemon["image"] = "registry.example.com:5000/acme/daemon:v1@sha256:" + "a" * 64
    daemon["selectorLabels"]["app.kubernetes.io/instance"] = "A_1.example"
    _assert_contract(daemon, valid=True)


@pytest.mark.parametrize("case", VECTOR["quantities"], ids=lambda case: case["name"])
@pytest.mark.parametrize("path", QUANTITY_PATHS, ids=lambda path: ".".join(path))
def test_quantity_vectors_match_model_parser_and_both_schemas(
    case: dict[str, Any], path: tuple[str, ...]
) -> None:
    daemon = copy.deepcopy(DAEMON)
    _set(daemon, path, case["value"])
    _assert_contract(daemon, valid=case["valid"])


@pytest.mark.parametrize("path", QUANTITY_PATHS, ids=lambda path: ".".join(path))
def test_worker_quantities_refuse_null_and_unbounded_literals(path: tuple[str, ...]) -> None:
    for value in (None, "1" * 4096 + "Gi"):
        daemon = copy.deepcopy(DAEMON)
        _set(daemon, path, value)
        _assert_contract(daemon, valid=False)


def test_model_validation_hides_hostile_field_values() -> None:
    marker = "DO_NOT_ECHO_daemon_field_1c47"
    daemon = copy.deepcopy(DAEMON)
    daemon["namespace"] = marker * 100
    with pytest.raises(ValidationError) as raised:
        ToolDaemonConfig.model_validate(daemon)
    assert marker not in str(raised.value)


@pytest.mark.parametrize(
    "order",
    [
        ["curie_worker.repo_runner_pools", "curie_worker.sandbox.daemon_config"],
        ["curie_worker.sandbox.daemon_config", "curie_worker.repo_runner_pools"],
    ],
    ids=["pool-first", "sandbox-first"],
)
def test_fresh_import_orders_can_parse_a_complete_daemon(order: list[str]) -> None:
    script = """
import importlib, json, sys
for name in json.loads(sys.argv[1]):
    importlib.import_module(name)
from curie_worker.repo_runner_pools import parse_repo_runner_pools
assert parse_repo_runner_pools(sys.stdin.read()).root
"""
    result = subprocess.run(
        [sys.executable, "-c", script, json.dumps(order)],
        input=json.dumps(_document(DAEMON)),
        text=True,
        capture_output=True,
        cwd=ROOT,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def _startup(raw: str) -> subprocess.CompletedProcess[str]:
    script = """
import json, sys
from curie_worker.run import _substrate_config
config = _substrate_config({"CURIE_REPO_RUNNER_POOLS": sys.stdin.read()})
print(json.dumps({key: value.model_dump(mode="json")
                  for key, value in config.repo_runner_pools.items()}))
"""
    return subprocess.run(
        [sys.executable, "-c", script],
        input=raw,
        text=True,
        capture_output=True,
        cwd=ROOT,
        timeout=30,
        check=False,
    )


@pytest.mark.parametrize("case", ["missing", "null", "value", "field", "repository", "json"])
def test_actual_startup_refuses_invalid_config_without_echoing_it(case: str) -> None:
    marker = "DO_NOT_ECHO_startup_daemon_16b3"
    document = _document(copy.deepcopy(DAEMON))
    if case == "missing":
        del document[REPO]["daemon"]
    elif case == "null":
        document[REPO]["daemon"] = None
    elif case == "value":
        document[REPO]["daemon"]["storageSizeLimit"] = marker * 100
    elif case == "field":
        document[REPO]["daemon"][marker] = "private payload"
    elif case == "repository":
        document = {f"/{marker}": document[REPO]}
    raw = f'{{"{marker}":' if case == "json" else json.dumps(document)
    result = _startup(raw)
    assert result.returncode != 0
    assert "CURIE_REPO_RUNNER_POOLS" in result.stderr
    assert marker not in result.stdout + result.stderr


@pytest.mark.parametrize("raw", ["", "{}"])
def test_actual_startup_keeps_unconfigured_repository_behavior(raw: str) -> None:
    result = _startup(raw)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {}


def _chart_values() -> dict[str, Any]:
    runner = {
        "tools": ["uv", "docker-cli"],
        "image": "registry.example.com/acme/runner@sha256:" + "a" * 64,
        "base": "sha256:" + "d" * 64,
        "platformDigests": {
            "linux/amd64": "sha256:" + "b" * 64,
            "linux/arm64": "sha256:" + "c" * 64,
        },
    }
    return {
        "factory": {
            "docker": {"daemonImage": DAEMON["image"]},
            "sandboxEgress": {"factory": True},
            "bindings": [
                {
                    "name": "acme",
                    "repos": [
                        {
                            "path": REPO,
                            "runner": copy.deepcopy(runner),
                            "docker": {"enabled": True},
                        },
                        {"path": "acme-corp/ui", "runner": copy.deepcopy(runner)},
                    ],
                }
            ],
        }
    }


def _render(
    values: dict[str, Any],
    *,
    skip_schema: bool = False,
    release: str = "t",
    namespace: str = "default",
) -> subprocess.CompletedProcess[str]:
    command = ["helm", "template", release, str(CHART), "--namespace", namespace, "-f", "-"]
    if skip_schema:
        command.append("--skip-schema-validation")
    return subprocess.run(
        command,
        input=json.dumps(values),
        text=True,
        capture_output=True,
        cwd=ROOT,
        timeout=30,
        check=False,
    )


def _worker_document(rendered: str) -> dict[str, Any]:
    docs = [doc for doc in yaml.safe_load_all(rendered) if doc]
    workers = [
        doc
        for doc in docs
        if doc.get("kind") == "Deployment"
        and doc["metadata"].get("labels", {}).get("app.kubernetes.io/component") == "worker"
    ]
    assert len(workers) == 1, "expected exactly one application worker Deployment"
    containers = [
        container
        for container in workers[0]["spec"]["template"]["spec"]["containers"]
        if container["name"] == "worker"
    ]
    assert len(containers) == 1, "expected exactly one application worker container"
    env = {entry["name"]: entry.get("value") for entry in containers[0].get("env", [])}
    return json.loads(env["CURIE_REPO_RUNNER_POOLS"])


def _values_validator() -> Any:
    schema = json.loads((CHART / "values.schema.json").read_text())
    return validator_for(schema)(schema)


def _assert_handoff(rendered: str, expected: dict[str, Any]) -> None:
    document = _worker_document(rendered)
    assert document[REPO]["daemon"] == expected
    assert type(document[REPO]["daemon"]["port"]) is int
    assert document["acme-corp/ui"]["docker"] is False
    assert "daemon" not in document["acme-corp/ui"]
    Draft202012Validator(build_schema()).validate(document)
    assert parse_repo_runner_pools(json.dumps(document)).model_dump(mode="json") == document
    result = _startup(json.dumps(document))
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == document


def test_retained_values_without_new_keys_emit_complete_defaults() -> None:
    result = _render(_chart_values())
    assert result.returncode == 0, result.stderr
    _assert_handoff(result.stdout, DAEMON)


def test_chart_root_and_every_daemon_override_reach_actual_startup() -> None:
    expected = copy.deepcopy(DAEMON)
    expected.update(
        namespace="acme-daemons",
        image="registry.example.com/acme/daemon@sha256:" + "8" * 64,
        port=2443,
        storageSizeLimit="5Gi",
        maxClaimLifetimeSeconds=3600,
        pingTimeoutSeconds=5,
        resources={
            "requests": {"cpu": "250m", "memory": "512Mi", "ephemeral-storage": "2Gi"},
            "limits": {"cpu": "1", "memory": "2Gi", "ephemeral-storage": "4Gi"},
        },
        selectorLabels={
            "app.kubernetes.io/name": "tool",
            "app.kubernetes.io/instance": "proof",
            "app.kubernetes.io/component": "docker-daemon",
        },
    )
    values = _chart_values()
    values["nameOverride"] = "tool"
    values["factory"]["docker"] = {
        key: copy.deepcopy(value)
        for key, value in expected.items()
        if key not in ("selectorLabels", "image")
    }
    values["factory"]["docker"].update(daemonImage=expected["image"], port="2443")
    result = _render(values, release="proof", namespace="acme-system")
    assert result.returncode == 0, result.stderr
    _assert_handoff(result.stdout, expected)


def test_chart_namespace_and_selectors_follow_the_release_root() -> None:
    values = _chart_values()
    values["nameOverride"] = "tool"
    expected = copy.deepcopy(DAEMON)
    expected["namespace"] = "acme-system-proof-tool-docker"
    expected["selectorLabels"].update(
        {"app.kubernetes.io/name": "tool", "app.kubernetes.io/instance": "proof"}
    )
    result = _render(values, release="proof", namespace="acme-system")
    assert result.returncode == 0, result.stderr
    _assert_handoff(result.stdout, expected)


@pytest.mark.parametrize(
    "resources",
    [
        None,
        {},
        {"requests": None},
        {"limits": None},
        {"requests": {"cpu": None}},
        {"requests": {"cpu": "250m"}},
        {"limits": {"memory": "2Gi"}},
    ],
)
def test_partial_or_coalesced_null_new_maps_receive_missing_leaf_defaults(resources: Any) -> None:
    values = _chart_values()
    values["factory"]["docker"]["resources"] = resources
    values["factory"]["docker"].update(
        storageSizeLimit=None, maxClaimLifetimeSeconds=None, pingTimeoutSeconds=None
    )
    expected = copy.deepcopy(DAEMON)
    for group, quantities in (resources or {}).items():
        expected["resources"][group].update(
            {key: value for key, value in (quantities or {}).items() if value is not None}
        )
    result = _render(values)
    assert result.returncode == 0, result.stderr
    _assert_handoff(result.stdout, expected)


@pytest.mark.parametrize("case", VECTOR["quantities"], ids=lambda case: case["name"])
@pytest.mark.parametrize("path", QUANTITY_PATHS, ids=lambda path: ".".join(path))
def test_shared_quantities_match_chart_values_schema_and_worker(
    case: dict[str, Any], path: tuple[str, ...]
) -> None:
    values = _chart_values()
    values["factory"]["docker"]["resources"] = copy.deepcopy(DAEMON["resources"])
    _set(values["factory"]["docker"], path, case["value"])
    validator = _values_validator()
    if case["valid"]:
        validator.validate(values)
    else:
        with pytest.raises(SchemaValidationError):
            validator.validate(values)
    for skip_schema in (False, True):
        result = _render(values, skip_schema=skip_schema)
        assert (result.returncode == 0) is case["valid"], result.stderr
        if case["valid"]:
            document = _worker_document(result.stdout)
            expected = copy.deepcopy(DAEMON)
            _set(expected, path, case["value"])
            assert document[REPO]["daemon"] == expected
            Draft202012Validator(build_schema()).validate(document)
            assert parse_repo_runner_pools(json.dumps(document)).model_dump(mode="json") == document


@pytest.mark.parametrize(
    ("path", "value"),
    [(path, "0") for path in QUANTITY_PATHS]
    + [
        (("resources",), False),
        (("resources",), "bad-map"),
        (("resources", "requests"), False),
        (("resources", "limits"), []),
        (("maxClaimLifetimeSeconds",), 0),
        (("maxClaimLifetimeSeconds",), False),
        (("maxClaimLifetimeSeconds",), "86400"),
        (("pingTimeoutSeconds",), -1),
        (("pingTimeoutSeconds",), True),
        (("pingTimeoutSeconds",), "2"),
    ],
)
def test_new_chart_values_reject_explicit_bad_inputs_instead_of_defaulting(
    path: tuple[str, ...], value: Any
) -> None:
    values = _chart_values()
    if path[0] == "resources":
        values["factory"]["docker"]["resources"] = copy.deepcopy(DAEMON["resources"])
    _set(values["factory"]["docker"], path, value)
    with pytest.raises(SchemaValidationError):
        _values_validator().validate(values)
    for skip_schema in (False, True):
        result = _render(values, skip_schema=skip_schema)
        assert result.returncode != 0
        # Schema validation names the key as a JSON pointer (Helm v3.20:
        # "- at '/factory/docker/maxClaimLifetimeSeconds': minimum: got 0, want 1");
        # the template's own fail() with schema skipped says "factory.docker...".
        assert re.search(r"factory[./]docker", result.stderr)


def test_values_schema_only_adds_constraints_to_new_keys() -> None:
    values = {
        "unknownExtension": True,
        "factory": {
            "unknownExtension": {},
            "docker": {
                "unknownExtension": [],
                "daemonImage": 17,
                "namespace": [],
                "port": "2376",
                "quota": False,
                "limitRange": "unchanged-schema-contract",
            },
        },
    }
    _values_validator().validate(values)


def test_disabled_worker_skips_enabled_daemon_validation_and_preserves_runner_documents() -> None:
    values = _chart_values()
    values["worker"] = {"deploy": False}
    del values["factory"]["docker"]["daemonImage"]
    result = _render(values)
    assert result.returncode == 0, result.stderr
    docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    assert not any(
        entry["name"] == "CURIE_REPO_RUNNER_POOLS"
        for doc in docs
        for container in (
            doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
        )
        for entry in container.get("env", [])
    )
    assert not any(
        doc.get("metadata", {})
        .get("labels", {})
        .get("app.kubernetes.io/component", "")
        .startswith("docker-daemon")
        for doc in docs
    )
    plain_values = copy.deepcopy(values)
    for repo in plain_values["factory"]["bindings"][0]["repos"]:
        repo.pop("docker", None)
    plain = _render(plain_values)
    assert plain.returncode == 0, plain.stderr
    for source in ("runner-resources-admission.yaml", "runner-prewarm.yaml", "security-probe.yaml"):
        marker = f"# Source: curie/templates/{source}\n"
        enabled_parts = [part for part in result.stdout.split("---\n") if part.startswith(marker)]
        plain_parts = [part for part in plain.stdout.split("---\n") if part.startswith(marker)]
        assert enabled_parts, source
        assert enabled_parts == plain_parts, source


_NAMESPACE_FAIL = "factory.docker.namespace must resolve to a Kubernetes namespace DNS label"
_SELECTOR_FAIL = "factory.docker selector labels must be nonempty Kubernetes label values"


def _selector_override(value: str) -> dict[str, Any]:
    values = _chart_values()
    values["factory"]["docker"]["namespace"] = "acme-docker"
    values["nameOverride"] = value
    return values


def _namespace_override(value: Any) -> dict[str, Any]:
    values = _chart_values()
    values["factory"]["docker"]["namespace"] = value
    return values


def _resources_override(group: str | None, key: str) -> dict[str, Any]:
    values = _chart_values()
    resources: dict[str, Any] = {key: "1"} if group is None else {group: {key: "1"}}
    values["factory"]["docker"]["resources"] = resources
    return values


@pytest.mark.parametrize(
    ("values", "message", "schema_rejects"),
    [
        (_namespace_override("Acme_Docker"), _NAMESPACE_FAIL, False),
        # The helper truncates to 63 characters, so a 64-character value alone resolves
        # validly; truncation leaving a doubled trailing dash is what must be refused.
        (_namespace_override("a" * 61 + "--b"), _NAMESPACE_FAIL, False),
        (_namespace_override(["acme-docker"]), "factory.docker.namespace must be a string", False),
        (_selector_override("-bad"), _SELECTOR_FAIL, False),
        (
            _resources_override(None, "extra"),
            "factory.docker.resources accepts only requests and limits",
            True,
        ),
        (
            _resources_override("requests", "gpu"),
            "factory.docker resources accept only cpu, memory and ephemeral-storage",
            True,
        ),
    ],
)
def test_chart_docker_config_refuses_with_its_own_message(
    values: dict[str, Any], message: str, schema_rejects: bool
) -> None:
    # The schema is deliberately permissive for namespace and selector inputs, so those
    # reach the template; resource keys are also rejected at schema time.
    if schema_rejects:
        with pytest.raises(SchemaValidationError):
            _values_validator().validate(values)
    else:
        _values_validator().validate(values)
    result = _render(values, skip_schema=schema_rejects)
    assert result.returncode != 0
    assert message in result.stderr


def test_chart_docker_config_accepts_a_valid_custom_namespace() -> None:
    # test_chart_root_and_every_daemon_override_reach_actual_startup covers the full
    # override set; this is the liveness neighbor of the namespace refusals above.
    result = _render(_namespace_override("acme-docker-1"))
    assert result.returncode == 0, result.stderr
    assert _worker_document(result.stdout)[REPO]["daemon"]["namespace"] == "acme-docker-1"
