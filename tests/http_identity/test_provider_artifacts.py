"""Standalone provider exports exercise the same independent wire examples.

@spec HTTP-IDENTITY-V1-5
"""

from __future__ import annotations

import copy
import importlib
import json
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[2]
VECTORS = json.loads((ROOT / "tests/vectors/http-identity-v1.json").read_text())
ARTIFACT_DIR = ROOT / "docs/interfaces/http-identity"


def exporter() -> ModuleType:
    name = "curie_api.http_identity_export"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "T1 standalone provider exporter is not implemented"
        raise
    for symbol in ("render_openapi", "render_json_schema"):
        assert callable(getattr(module, symbol, None)), f"T1 exporter entry point missing: {symbol}"
    return module


def dereference(document: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any]:
    while "$ref" in schema:
        ref = schema["$ref"]
        assert ref.startswith("#/"), (
            "provider schema must be standalone, without remote dependencies"
        )
        schema = document
        for key in ref[2:].split("/"):
            schema = schema[key.replace("~1", "/").replace("~0", "~")]
    return schema


def schema_validator(document: dict[str, Any], shape: dict[str, Any]) -> Draft202012Validator:
    # Keep the root definitions available to local references from either export.
    schema = {**document, **shape}
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


@pytest.mark.parametrize(
    ("renderer", "filename"),
    [
        ("render_openapi", "http-identity-v1.openapi.json"),
        ("render_json_schema", "http-identity-v1.schema.json"),
    ],
)
def test_export_is_deterministic_and_committed_artifact_has_no_drift(
    renderer: str, filename: str
) -> None:
    """@spec HTTP-IDENTITY-V1-5."""
    module = exporter()
    render = getattr(module, renderer)
    first = render()
    assert isinstance(first, bytes)
    assert first == render()
    json.loads(first)
    path = ARTIFACT_DIR / filename
    assert path.is_file(), f"missing generated provider artifact: {filename}"
    assert first == path.read_bytes()


@pytest.mark.parametrize("format_name", ["openapi", "json_schema"])
def test_exported_shapes_accept_independent_union_vectors(format_name: str) -> None:
    """@spec HTTP-IDENTITY-V1-1 HTTP-IDENTITY-V1-2 HTTP-IDENTITY-V1-5."""
    module = exporter()
    document = json.loads(getattr(module, f"render_{format_name}")())
    if format_name == "openapi":
        assert document["openapi"].startswith("3.1.")
        assert set(document["paths"]) == {"/lookup"}
        operation = document["paths"]["/lookup"]["post"]
        request_shape = operation["requestBody"]["content"]["application/json"]["schema"]
        response_shape = operation["responses"]["200"]["content"]["application/json"]["schema"]
    else:
        assert document["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        request_shape = {"$ref": "#/$defs/LookupRequestV1"}
        response_shape = {"$ref": "#/$defs/LookupResponseV1"}
    request_validator = schema_validator(document, dereference(document, request_shape))
    response_validator = schema_validator(document, dereference(document, response_shape))
    request_validator.validate(VECTORS["request"])
    for case in VECTORS["cases"]:
        response_validator.validate(case["response"])


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("unexpected", "field"),
        ("schema_version", 2),
        ("schema_version", True),
        ("authority_version", "7"),
        ("purpose", "Approval"),
        ("outcome", "absent"),
        ("identity", None),
        ("membership", None),
        ("sender_kind", "unknown"),
    ],
)
@pytest.mark.parametrize("format_name", ["openapi", "json_schema"])
def test_exported_shapes_reject_expressible_broken_vectors(
    format_name: str,
    field: str,
    replacement: Any,
) -> None:
    """@spec HTTP-IDENTITY-V1-2 HTTP-IDENTITY-V1-3 HTTP-IDENTITY-V1-5."""
    module = exporter()
    document = json.loads(getattr(module, f"render_{format_name}")())
    if format_name == "openapi":
        shape = document["paths"]["/lookup"]["post"]["responses"]["200"]["content"][
            "application/json"
        ]["schema"]
    else:
        shape = {"$ref": "#/$defs/LookupResponseV1"}
    validator = schema_validator(document, dereference(document, shape))
    response = copy.deepcopy(VECTORS["cases"][0]["response"])
    response[field] = replacement
    assert not validator.is_valid(response), f"export accepts broken {field}"


def test_provider_openapi_records_transport_obligations_without_implementing_them() -> None:
    """@spec HTTP-IDENTITY-V1-5 HTTP-IDENTITY-V1-6."""
    document = json.loads(exporter().render_openapi())
    obligations = document["x-curie-transport"]
    assert obligations["total_deadline_ms"] == 2000
    assert obligations["retries"] == 0
    assert obligations["max_response_bytes"] == 65536
    assert obligations["follow_redirects"] is False
    assert obligations["positive_identity_cache"] is False
    assert obligations["transport_implemented"] is False
