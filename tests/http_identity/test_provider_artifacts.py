"""Standalone provider exports exercise the same independent wire examples.

@spec HTTP-IDENTITY-V1-5
"""

from __future__ import annotations

import copy
import importlib
import json
from datetime import datetime
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


def artifact_format_checker() -> FormatChecker:
    checker = FormatChecker()

    @checker.checks("date-time", raises=ValueError)
    def calendar_instant(value: Any) -> bool:
        # jsonschema's optional RFC3339 checker is absent in the workspace.
        # The exported pattern owns syntax; stdlib checks real calendar/time values
        # without interpreting or rounding fractional precision for freshness.
        if isinstance(value, str):
            datetime.fromisoformat(value)
        return True

    return checker


def schema_validator(document: dict[str, Any], shape: dict[str, Any]) -> Draft202012Validator:
    # Keep the root definitions available to local references from either export.
    schema = {**document, **shape}
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=artifact_format_checker())


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


def artifact_model_validator(format_name: str, model_name: str) -> Draft202012Validator:
    module = exporter()
    document = json.loads(getattr(module, f"render_{format_name}")())
    if format_name == "openapi":
        operation = document["paths"]["/lookup"]["post"]
        if model_name == "LookupRequestV1":
            shape = operation["requestBody"]["content"]["application/json"]["schema"]
        else:
            shape = operation["responses"]["200"]["content"]["application/json"]["schema"]
    else:
        shape = {"$ref": f"#/$defs/{model_name}"}
    return schema_validator(document, dereference(document, shape))


def issuer_example(model_name: str, issuer: str) -> dict[str, Any]:
    if model_name == "LookupRequestV1":
        example = copy.deepcopy(VECTORS["request"])
    else:
        example = copy.deepcopy(VECTORS["cases"][0]["response"])
        example["identity"]["issuer"] = issuer
        example["membership"]["groups"][0]["issuer"] = issuer
    example["scope"]["expected_issuer"] = issuer
    return example


@pytest.mark.parametrize("format_name", ["openapi", "json_schema"])
@pytest.mark.parametrize(
    ("model_name", "issuer_path"),
    [
        ("LookupRequestV1", "scope.expected_issuer"),
        ("LookupResponseV1", "scope.expected_issuer"),
        ("LookupResponseV1", "identity.issuer"),
        ("LookupResponseV1", "membership.groups.0.issuer"),
    ],
)
@pytest.mark.parametrize(
    "issuer",
    [
        "",
        "http://identity.example.test",
        "https:///directory",
        "https://user@identity.example.test/directory",
        "https://identity.example.test/directory?query=1",
        "https://identity.example.test/directory#fragment",
        " https://identity.example.test/directory",
        "https://identity.example.test/directory ",
        "https://[not-ip]",
        "https://[127.0.0.1]",
        "https://[vXYZ.test]",
        "https://[1:2:3:4:5:6:7]",
        "https://[1:2:3:4:5:6:7:8:9]",
        "https://[2001::db8::1]",
        "https://[::ffff:192.0.2.999]",
        "https://[::ffff:192.0.2]",
        "https://[fe80::1%]",
        "https://[fe80::1%eth0%extra]",
        "https://[v1.]",
        "\thttps://identity.example.test",
        "https://identity.example.test\r",
    ],
    ids=[
        "empty",
        "http",
        "missing-host",
        "userinfo",
        "query",
        "fragment",
        "leading-space",
        "trailing-space",
        "bracketed-non-ip",
        "bracketed-ipv4",
        "invalid-ipvfuture-version",
        "ipv6-too-few-groups",
        "ipv6-too-many-groups",
        "ipv6-double-compression",
        "embedded-ipv4-invalid-octet",
        "embedded-ipv4-too-short",
        "ipv6-empty-zone",
        "ipv6-repeated-zone-separator",
        "ipvfuture-empty-address",
        "leading-tab",
        "trailing-carriage-return",
    ],
)
def test_artifact_issuer_constraints_agree_with_strict_models(
    format_name: str,
    model_name: str,
    issuer_path: str,
    issuer: str,
) -> None:
    """@spec HTTP-IDENTITY-V1-1 HTTP-IDENTITY-V1-5."""
    model = getattr(importlib.import_module("curie_api.schemas.http_identity"), model_name)
    example = issuer_example(model_name, "https://identity.example.test/directory")
    target = example
    parts = issuer_path.split(".")
    for part in parts[:-1]:
        target = target[int(part)] if isinstance(target, list) else target[part]
    target[parts[-1]] = issuer
    with pytest.raises(ValueError):
        model.model_validate(example)
    assert not artifact_model_validator(format_name, model_name).is_valid(example), (
        f"{format_name} accepts model-invalid issuer {issuer!r}"
    )


@pytest.mark.parametrize("format_name", ["openapi", "json_schema"])
@pytest.mark.parametrize("model_name", ["LookupRequestV1", "LookupResponseV1"])
@pytest.mark.parametrize(
    "issuer",
    [
        "https://identity.example.test/directory",
        "HTTPS://IDENTITY.EXAMPLE.TEST/Directory",
        "https://bücher.example.test/directory",
        "https://identity.example.test/目录",
        "https://identity.example.test:443/directory",
        "https://[::1]",
        "https://[2001:db8::1]",
        "https://[2001:db8:0:0:0:0:2:1]",
        "https://[::ffff:192.0.2.128]",
        "https://[2001:db8::192.0.2.33]",
        "https://[fe80::1%25eth0]",
        "https://[fe80::1%eth0]",
        "https://[v1.test]",
        "https://[::1]:443/directory",
        # Existing model grammar ignores these controls for URL parsing while
        # retaining the complete original issuer as the exact identity value.
        "h\tttps://identity.example.test",
        "ht\rtps://identity.example.test",
        "htt\nps://identity.example.test",
        "\x00https://identity.example.test",
        "\x01https://identity.example.test",
        "https://identity.exa\tmple.test/directory",
        "https://identity.example.test/di\nrectory",
        "https:\r//identity.example.test/directory",
    ],
    ids=[
        "https",
        "uppercase-scheme",
        "unicode-host",
        "unicode-path",
        "explicit-port",
        "ipv6-loopback",
        "ipv6-compressed",
        "ipv6-full",
        "ipv4-mapped-ipv6",
        "ipv6-embedded-ipv4",
        "ipv6-encoded-zone",
        "ipv6-raw-zone",
        "ipvfuture",
        "ipv6-explicit-port",
        "scheme-tab",
        "scheme-carriage-return",
        "scheme-newline",
        "leading-nul",
        "leading-soh",
        "host-tab",
        "path-newline",
        "delimiter-carriage-return",
    ],
)
def test_artifact_issuer_constraints_preserve_model_valid_exact_values(
    format_name: str,
    model_name: str,
    issuer: str,
) -> None:
    """@spec HTTP-IDENTITY-V1-1 HTTP-IDENTITY-V1-5."""
    model = getattr(importlib.import_module("curie_api.schemas.http_identity"), model_name)
    example = issuer_example(model_name, issuer)
    validated = model.model_validate(example)
    assert validated.model_dump(mode="json") == example
    artifact_model_validator(format_name, model_name).validate(example)


@pytest.mark.parametrize("format_name", ["openapi", "json_schema"])
@pytest.mark.parametrize("observation", ["identity", "membership"])
@pytest.mark.parametrize(
    "timestamp",
    [
        "2030-01-01T99:99:99Z",
        "2030-02-30T12:00:00Z",
        "2030-13-01T12:00:00Z",
        "2030-01-00T12:00:00Z",
        "0000-01-01T12:00:00Z",
        "2030-01-01T12:00:60Z",
    ],
    ids=["impossible-clock", "february-30", "month-13", "day-zero", "year-zero", "leap-second"],
)
def test_artifact_timestamp_constraints_reject_model_invalid_calendar_instants(
    format_name: str,
    observation: str,
    timestamp: str,
) -> None:
    """@spec HTTP-IDENTITY-V1-4 HTTP-IDENTITY-V1-5."""
    model = importlib.import_module("curie_api.schemas.http_identity").LookupResponseV1
    example = copy.deepcopy(VECTORS["cases"][0]["response"])
    target = (
        example["identity_observation"]
        if observation == "identity"
        else example["membership"]["observation"]
    )
    target["as_of"] = timestamp
    with pytest.raises(ValueError):
        model.model_validate(example)
    assert not artifact_model_validator(format_name, "LookupResponseV1").is_valid(example), (
        f"{format_name} accepts model-invalid calendar instant {timestamp!r}"
    )


@pytest.mark.parametrize("format_name", ["openapi", "json_schema"])
@pytest.mark.parametrize("observation", ["identity", "membership"])
@pytest.mark.parametrize(
    "timestamp",
    [
        "2030-01-01T12:00:00Z",
        "2030-01-01T12:00:00+00:00",
        "2032-02-29T23:59:59Z",
        "2030-01-01T12:00:00.1234567890123456789Z",
        "2030-01-01T12:00:00." + "0" * 4400 + "1Z",
    ],
    ids=["utc-z", "utc-offset", "leap-day", "fraction", "long-fraction"],
)
def test_artifact_timestamp_constraints_preserve_valid_calendar_and_precision(
    format_name: str,
    observation: str,
    timestamp: str,
) -> None:
    """@spec HTTP-IDENTITY-V1-4 HTTP-IDENTITY-V1-5."""
    model = importlib.import_module("curie_api.schemas.http_identity").LookupResponseV1
    example = copy.deepcopy(VECTORS["cases"][0]["response"])
    target = (
        example["identity_observation"]
        if observation == "identity"
        else example["membership"]["observation"]
    )
    target["as_of"] = timestamp
    validated = model.model_validate(example)
    assert validated.model_dump(mode="json") == example
    artifact_model_validator(format_name, "LookupResponseV1").validate(example)
