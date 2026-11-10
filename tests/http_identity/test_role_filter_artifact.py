"""Independent structural export checks; byte/semantic parsing remains separate.

@spec AGENT-ROLE-FILTER-V1-1 AGENT-ROLE-FILTER-V1-6 AGENT-ROLE-FILTER-V1-7
"""

from __future__ import annotations

import copy
import importlib
import json
import re
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
VECTORS = json.loads((ROOT / "tests/vectors/agent-role-filter-v1.json").read_text())
ARTIFACT = ROOT / "docs/interfaces/agent-access/agent-role-filter-v1.schema.json"
INVALID = "invalid_agent_role_configuration"
UNSUPPORTED = "unsupported_agent_role_configuration_version"
ROOT_MODELS = {"policy": "AgentRolePolicyV1", "change": "AgentRoleChangeV1"}


def exporter() -> ModuleType:
    """@spec AGENT-ROLE-FILTER-V1-7: resolve absent behavior inside test bodies."""
    name = "curie_api.agent_role_filter_export"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "standalone agent role filter exporter is not implemented"
        raise
    for symbol in ("render_json_schema", "write_artifact"):
        assert callable(getattr(module, symbol, None)), f"exporter entry point missing: {symbol}"
    return module


def document() -> dict[str, Any]:
    """@spec AGENT-ROLE-FILTER-V1-7."""
    raw = exporter().render_json_schema()
    assert isinstance(raw, bytes)
    return json.loads(raw)


def validator(target: str) -> Draft202012Validator:
    """@spec AGENT-ROLE-FILTER-V1-7: retain both root definitions for local refs."""
    schema = document()
    model = ROOT_MODELS[target]
    assert model in schema.get("$defs", {}), f"missing standalone root definition: {model}"
    schema = {**schema, "$ref": f"#/$defs/{model}"}
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def candidate(target: str = "policy") -> dict[str, Any]:
    """@spec AGENT-ROLE-FILTER-V1-7."""
    return copy.deepcopy(next(c["input"] for c in VECTORS["cases"] if c["target"] == target))


def encoded(value: Any) -> bytes:
    """@spec AGENT-ROLE-FILTER-V1-6 AGENT-ROLE-FILTER-V1-7."""
    return json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode()


def parse(raw: bytes, target: str) -> Any:
    """@spec AGENT-ROLE-FILTER-V1-6 AGENT-ROLE-FILTER-V1-7."""
    module = importlib.import_module("curie_api.agent_role_filter")
    return getattr(module, f"parse_agent_role_{target}_v1")(raw)


def refuse(raw: bytes, target: str, code: str = INVALID) -> None:
    """@spec AGENT-ROLE-FILTER-V1-6 AGENT-ROLE-FILTER-V1-7."""
    result = parse(raw, target)
    model = importlib.import_module("curie_api.schemas.agent_access").RoleFilterRefusal
    assert isinstance(result, model)
    assert result.model_dump(mode="json") == {"code": code}


def nodes(value: Any) -> Iterator[dict[str, Any]]:
    """@spec AGENT-ROLE-FILTER-V1-7."""
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from nodes(item)
    elif isinstance(value, list):
        for item in value:
            yield from nodes(item)


def test_export_deterministic_and_no_drift() -> None:
    """@spec AGENT-ROLE-FILTER-V1-7."""
    module = exporter()
    first = module.render_json_schema()
    assert isinstance(first, bytes)
    assert first == module.render_json_schema() == module.render_json_schema()
    assert first.endswith(b"\n")
    schema = json.loads(first)
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["$id"] == "https://curietech.ai/schemas/agent-role-filter-v1.schema.json"
    assert not any({"generated_at", "timestamp"} & set(node) for node in nodes(schema))
    Draft202012Validator.check_schema(schema)
    assert ARTIFACT.is_file(), "missing generated standalone agent role filter schema"
    assert ARTIFACT.read_bytes() == first


@pytest.mark.parametrize("case", VECTORS["cases"], ids=lambda c: c["name"])
def test_shared_vectors_match_structural_schema(case: dict[str, Any]) -> None:
    """@spec AGENT-ROLE-FILTER-V1-7: schema acceptance and parser result are independent."""
    assert validator(case["target"]).is_valid(case["input"]) is case["schema_valid"]
    raw = encoded(case["input"])
    if case["expected"] != "valid":
        refuse(raw, case["target"], case["expected"])
    else:
        result = parse(raw, case["target"])
        assert result.model_dump(mode="json") == case["input"]
    parser_valid = case["expected"] == "valid"
    if parser_valid != case["schema_valid"]:
        assert case.get("exception") in {
            "integer-token spelling",
            "inherited Issuer Unicode-NFKC validity",
            "unpaired-surrogate rejection",
            "namespace consistency",
        }
    else:
        assert "exception" not in case


@pytest.mark.parametrize("target", ["policy", "change"])
def test_artifact_has_local_references_and_no_runtime_route(target: str) -> None:
    """@spec AGENT-ROLE-FILTER-V1-1 AGENT-ROLE-FILTER-V1-7."""
    schema = document()
    assert set(ROOT_MODELS.values()) <= set(schema["$defs"])
    assert not {"openapi", "paths", "components"} & set(schema)
    references = [node["$ref"] for node in nodes(schema) if "$ref" in node]
    assert references
    for ref in references:
        assert ref.startswith("#/$defs/"), f"external schema reference: {ref}"
        current = schema
        for key in ref[2:].split("/"):
            current = current[key.replace("~1", "/").replace("~0", "~")]
        assert isinstance(current, dict)
    validator(target).validate(candidate(target))


def test_exported_collection_limit_and_exact_duplicate_constraint() -> None:
    """@spec AGENT-ROLE-FILTER-V1-6 AGENT-ROLE-FILTER-V1-7."""
    schema = document()
    shape = schema["$defs"]["RoleFilterV1"]["properties"]["entries"]
    assert shape["maxItems"] == 128
    assert shape["uniqueItems"] is True
    value = candidate()
    role_filter = value["roles"]["invocation"]["filter"]
    entries = [
        {"kind": "issuer_subject", "issuer": "https://identity.example.test", "subject": str(i)}
        for i in range(128)
    ]
    role_filter["entries"] = entries
    validator("policy").validate(value)
    role_filter["entries"] = entries + [dict(entries[0], subject="128")]
    assert not validator("policy").is_valid(value)
    role_filter["entries"] = entries[:1] * 2
    assert not validator("policy").is_valid(value)


@pytest.mark.parametrize(
    ("target", "field"),
    [
        ("policy", "schema_version"),
        ("policy", "version"),
        ("policy", "nested_schema"),
        ("change", "schema_version"),
        ("change", "expected_version"),
    ],
)
@pytest.mark.parametrize("token", [b"1.0", b"1e0"])
def test_integer_spelling_is_schema_accepted_and_parser_refused(
    target: str, field: str, token: bytes
) -> None:
    """@spec AGENT-ROLE-FILTER-V1-7: decoded schema integers cannot enforce JSON tokens."""
    value = candidate(target)
    raw = encoded(value)
    if field == "nested_schema":
        old = b'"roles":{"invocation":{"schema_version":1'
        new = b'"roles":{"invocation":{"schema_version":' + token
    else:
        old = encoded(field) + b":" + str(value[field]).encode()
        new = encoded(field) + b":" + token
    assert old in raw
    raw = raw.replace(old, new, 1)
    validator(target).validate(json.loads(raw))
    refuse(raw, target)


def test_semantic_only_constraints_are_explicit() -> None:
    """@spec AGENT-ROLE-FILTER-V1-6 AGENT-ROLE-FILTER-V1-7."""
    exceptions = {case["exception"] for case in VECTORS["cases"] if "exception" in case}
    assert exceptions == {
        "integer-token spelling",
        "inherited Issuer Unicode-NFKC validity",
        "unpaired-surrogate rejection",
        "namespace consistency",
    }
    # Byte limits and duplicate JSON keys are absent from decoded JSON Schema.
    # Integer-token spelling, namespace consistency, Issuer NFKC checks and
    # surrogate rejection are pinned separately by the shared decoded vectors.
    # Refusal-code precedence is a parser-only result, not a schema verdict.
    value = candidate()
    raw = encoded(value)
    oversized = raw + b" " * (65537 - len(raw))
    validator("policy").validate(json.loads(oversized))
    refuse(oversized, "policy")
    duplicated = raw.replace(b'"schema_version":1', b'"schema_version":1,"schema_version":1', 1)
    validator("policy").validate(json.loads(duplicated))
    refuse(duplicated, "policy")
    unsupported = dict(value, schema_version=2, extra="SyntheticExtra")
    assert not validator("policy").is_valid(unsupported)
    refuse(encoded(unsupported), "policy", UNSUPPORTED)
    unsupported["schema_version"] = 1
    unsupported["roles"]["invocation"]["schema_version"] = 2
    assert not validator("policy").is_valid(unsupported)
    refuse(encoded(unsupported), "policy")


@pytest.mark.parametrize("target", ["policy", "change"])
def test_required_forbidden_and_discriminator_constraints(target: str) -> None:
    """@spec AGENT-ROLE-FILTER-V1-7."""
    value = candidate(target)
    check = validator(target)
    for field in value:
        broken = copy.deepcopy(value)
        del broken[field]
        assert not check.is_valid(broken), f"schema made required {field} optional"
    assert not check.is_valid(dict(value, ready=True))
    if target == "policy":
        value["roles"] = {"unexpected": {}}
        assert not check.is_valid(value)
        value = candidate()
        value["roles"]["invocation"] = None
        assert not check.is_valid(value)
        value = candidate()
        value["roles"]["invocation"]["filter"]["entries"][0]["kind"] = "unknown_selector"
        assert not check.is_valid(value)
    else:
        assert not check.is_valid(dict(value, operation="reverse_migration"))
        assert not check.is_valid(dict(value, role="approval"))
        migrated = dict(
            value, operation="migrate_role", raw_account_revocation="separate_removal_required"
        )
        check.validate(migrated)
        assert not check.is_valid(dict(migrated, raw_account_revocation="bypass"))


def test_rendering_preserves_existing_provider_and_api_artifacts() -> None:
    """@spec AGENT-ROLE-FILTER-V1-1 AGENT-ROLE-FILTER-V1-7."""
    protected = [
        ROOT / "apps/api/openapi.json",
        *sorted((ROOT / "docs/interfaces/http-identity").glob("*.json")),
    ]
    assert len(protected) >= 3
    before = {path: path.read_bytes() for path in protected}
    directory_before = set((ROOT / "docs/interfaces").rglob("*.json"))
    module = exporter()
    for _ in range(2):
        module.render_json_schema()
    assert {path: path.read_bytes() for path in protected} == before
    assert set((ROOT / "docs/interfaces").rglob("*.json")) == directory_before
    assert all(
        b"AgentRolePolicyV1" not in data and b"AgentRoleChangeV1" not in data
        for data in before.values()
    )


def test_existing_contracts_regeneration_includes_export_and_drift_path() -> None:
    """@spec AGENT-ROLE-FILTER-V1-7: existing command owns regeneration."""
    script = (ROOT / "scripts/check-contracts.sh").read_text()
    assert re.search(
        r"^uv run python -m curie_api\.agent_role_filter_export\s*$", script, re.MULTILINE
    ), "existing contracts regeneration omits role filter exporter"
    relative = ARTIFACT.relative_to(ROOT).as_posix()
    before_drift, drift = script.split('echo "== checking for drift =="', 1)
    assignments = re.findall(
        r'^([A-Za-z_][A-Za-z_0-9]*)="' + re.escape(relative) + r'"$', before_drift, re.MULTILINE
    )
    assert relative in drift or any(f'"${name}"' in drift for name in assignments), (
        "role filter artifact is absent from the contracts drift check"
    )


def test_export_import_is_inert() -> None:
    """@spec AGENT-ROLE-FILTER-V1-1 AGENT-ROLE-FILTER-V1-7."""
    exporter()  # Missing implementation is an ordinary test assertion.
    script = r"""
import builtins
import importlib.abc
import io
import os
from pathlib import Path
import socket
import sys
forbidden = ("curie_api.main", "curie_api.app", "curie_api.config", "curie_api.db",
             "curie_api.models", "curie_api.agent_access", "curie_api.routers",
             "curie_api.http_identity_export", "curie_api.export_openapi",
             "curie_api.team_authorization", "sqlalchemy", "asyncpg", "httpx",
             "requests", "redis", "boto3", "kubernetes", "curie_worker")
class RuntimeForbidden(importlib.abc.MetaPathFinder):
    # @spec AGENT-ROLE-FILTER-V1-1 AGENT-ROLE-FILTER-V1-7
    def find_spec(self, fullname, path=None, target=None):
        # @spec AGENT-ROLE-FILTER-V1-1 AGENT-ROLE-FILTER-V1-7
        if any(fullname == p or fullname.startswith(p + ".") for p in forbidden):
            raise AssertionError("pure exporter imported runtime: " + fullname)
sys.meta_path.insert(0, RuntimeForbidden())
def no_io(*args, **kwargs):
    # @spec AGENT-ROLE-FILTER-V1-1 AGENT-ROLE-FILTER-V1-7
    raise AssertionError("exporter import/render attempted IO")
Path.mkdir = Path.write_bytes = Path.write_text = no_io
socket.socket = socket.create_connection = socket.getaddrinfo = no_io
from curie_api.agent_role_filter_export import render_json_schema, write_artifact
builtins.open = io.open = os.open = no_io
first = render_json_schema()
assert isinstance(first, bytes) and first == render_json_schema()
assert callable(write_artifact)
assert not any(n == p or n.startswith(p + ".") for n in sys.modules for p in forbidden)
print("pure exporter import and deterministic rendering passed")
"""
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=False, timeout=20
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "pure exporter import and deterministic rendering passed"


@pytest.mark.parametrize(
    ("target", "path"),
    [
        ("policy", "tenant_id"),
        ("policy", "agent_id"),
        ("policy", "roles.invocation.filter.entries.0.namespace.id"),
        ("change", "tenant_id"),
        ("change", "agent_id"),
        ("change", "filter.entries.0.namespace.id"),
    ],
)
@pytest.mark.parametrize(
    ("literal", "valid"),
    [
        pytest.param("44444444-4444-4444-4444-444444444444", True, id="canonical-numeric"),
        pytest.param("abcdefab-cdef-abcd-efab-cdefabcdefab", True, id="canonical-alphabetic"),
        pytest.param("not-a-uuid", False, id="malformed"),
        pytest.param("abcdefabcdefabcdefabcdefabcdefab", False, id="hyphenless"),
        pytest.param("ABCDEFAB-CDEF-ABCD-EFAB-CDEFABCDEFAB", False, id="uppercase"),
        pytest.param("abcdefab-cdef-abcd-efab-cdefabcdefab\n", False, id="trailing-newline"),
    ],
)
def test_canonical_uuid_fields_agree_with_runtime_parser(
    target: str, path: str, literal: str, valid: bool
) -> None:
    """@spec AGENT-ROLE-FILTER-V1-2 AGENT-ROLE-FILTER-V1-7: imported exact UUID shape."""
    value = candidate(target)
    keys = path.split(".")
    node = value
    for key in keys[:-1]:
        node = node[int(key)] if isinstance(node, list) else node[key]
    node[keys[-1]] = literal
    if valid:
        assert parse(encoded(value), target).model_dump(mode="json") == value
        validator(target).validate(value)
    else:
        refuse(encoded(value), target)
        assert not validator(target).is_valid(value), (
            f"structural schema accepts noncanonical UUID at {target}.{path}"
        )
