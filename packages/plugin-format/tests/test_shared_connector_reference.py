"""Shared declarations validate without activating a dedicated connector."""

import json
from pathlib import Path

import jsonschema
import pytest
from plugin_format import connector_render, validate_bundle
from plugin_format.connectors import (
    ConnectorsFile,
    ConnectorSpec,
    SharedConnectorReference,
    validate_connectors,
)
from plugin_format.schema_export import build_schema
from pydantic import ValidationError

CASES = json.loads(
    (Path(__file__).parents[3] / "tests/vectors/shared-connector-reference.json").read_text()
)["vectors"]
REFERENCE = CASES[0]["document"]["connectors"]["docs"]["shared"]
APPLICATION = REFERENCE["application"]


def _document(case: dict) -> dict:
    if "sibling" in case:
        return {"connectors": {"docs": {"shared": REFERENCE, **case["sibling"]}}}
    return case["document"]


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_shared_connector_reference_vectors(case: dict) -> None:
    document = _document(case)
    activated, errors = validate_connectors(document)
    assert (not errors) is case["activation"], case["name"]
    assert (activated is not None) is case["activation"], case["name"]
    if not case["accept"]:
        with pytest.raises(ValidationError):
            ConnectorsFile.model_validate(document)
        assert errors, case["name"]
        return
    parsed = ConnectorsFile.model_validate(document)
    if not case["activation"]:
        assert {code.rsplit(".", 1)[-1] for code, _ in errors} == {
            "shared_connector_requires_application"
        }
    for alias, declaration in document["connectors"].items():
        spec = parsed.connectors[alias]
        if "shared" in declaration:
            assert spec.shared.model_dump() == REFERENCE
        elif "build" in declaration:
            assert spec.build.context == declaration["build"]["context"]
        else:
            field = "image" if "image" in declaration else "url"
            assert getattr(spec, field) == declaration[field]


@pytest.mark.parametrize(
    "case", [case for case in CASES if case["activation"]], ids=lambda case: case["name"]
)
def test_dedicated_model_dump_omits_shared_and_roundtrips(case: dict) -> None:
    spec = ConnectorSpec.model_validate(_document(case)["connectors"]["docs"])
    dumped = spec.model_dump()
    assert "shared" not in dumped
    assert ConnectorSpec.model_validate(dumped) == spec


@pytest.mark.parametrize(
    "case",
    [case for case in CASES if case["accept"] and not case["activation"]],
    ids=lambda case: case["name"],
)
def test_shared_and_mixed_serialization_roundtrips_exclusive_shape(case: dict) -> None:
    document = _document(case)
    bundle = ConnectorsFile.model_validate(document)
    for serialized in (bundle.model_dump(), json.loads(bundle.model_dump_json())):
        assert ConnectorsFile.model_validate(serialized) == bundle
        for alias, declaration in document["connectors"].items():
            if "shared" in declaration:
                spec = bundle.connectors[alias]
                assert spec.model_dump() == declaration
                assert json.loads(spec.model_dump_json()) == declaration
                assert serialized["connectors"][alias] == declaration
                assert ConnectorSpec.model_validate(spec.model_dump()) == spec
                assert ConnectorSpec.model_validate_json(spec.model_dump_json()) == spec
    assert ConnectorsFile.model_validate_json(bundle.model_dump_json()) == bundle


@pytest.mark.parametrize(
    "call",
    [
        lambda spec: connector_render.caller_proxy_port(spec),
        lambda spec: connector_render.resolved_admits(spec, "acme-bot"),
        lambda spec: connector_render.render_service("acme", "acme-bot", "docs", spec),
        lambda spec: connector_render.render_direct_service("acme", "acme-bot", "docs", spec),
        lambda spec: connector_render.render_deployment(
            "acme", "acme-bot", "acme", "docs", spec, "acme-secrets"
        ),
        lambda spec: connector_render.render_networkpolicy(
            "acme", "acme-bot", "curie", "docs", spec
        ),
        lambda spec: connector_render.render_ingress_networkpolicy(
            "acme", "acme-bot", "curie", "docs", spec
        ),
    ],
    ids=["proxy-port", "admits", "service", "direct-service", "deployment", "egress", "ingress"],
)
def test_direct_dedicated_render_helpers_refuse_shared(call) -> None:
    with pytest.raises(ValueError, match="shared_connector_requires_application"):
        call(ConnectorSpec(shared=REFERENCE))


# JSON Schema regards 1 and 1.0 as equal; both production parsers enforce strict integers.
@pytest.mark.parametrize(
    "case",
    [case for case in CASES if case["name"] != "float_contract"],
    ids=lambda case: case["name"],
)
def test_shared_connector_schema_vectors(case: dict) -> None:
    schema = {**build_schema(), "$ref": "#/$defs/ConnectorsFile"}
    errors = list(jsonschema.Draft202012Validator(schema).iter_errors(_document(case)))
    assert (not errors) is case["accept"], case["name"]


@pytest.mark.parametrize(
    "changes",
    [
        {"application": APPLICATION.upper()},
        {"contract": True},
        {"contract": 1.0},
        {"contract": "1"},
    ],
)
def test_shared_reference_validates_on_construction(changes: dict) -> None:
    with pytest.raises(ValidationError):
        SharedConnectorReference(**(REFERENCE | changes))


def test_shared_reference_identity_is_immutable() -> None:
    reference = SharedConnectorReference(**REFERENCE)
    with pytest.raises(ValidationError):
        reference.application = "22345678-1234-4234-8234-123456789abc"
    assert reference.model_dump() == REFERENCE


@pytest.mark.parametrize(
    "helper",
    [
        "render", "mcp_entry", "unhosted_mcp_entry",
        "secret_names", "resolved_secrets", "server_resources",
    ],
)
def test_shared_activation_helpers_refuse_without_application(helper: str) -> None:
    spec = ConnectorSpec(shared=REFERENCE)
    with pytest.raises(ValueError, match="shared_connector_requires_application"):
        if helper == "render":
            connector_render.render(
                release="acme",
                agent="acme-bot",
                namespace="acme",
                app_name="curie",
                connector="docs",
                spec=spec,
                secret_name="acme-secrets",
            )
        elif helper == "mcp_entry":
            connector_render.mcp_entry("acme", "acme-bot", "acme", "docs", spec)
        elif helper == "unhosted_mcp_entry":
            connector_render.unhosted_mcp_entry(spec)
        else:
            getattr(spec, helper)()


def test_dedicated_helpers_keep_working_beside_shared() -> None:
    spec = ConnectorSpec(url="https://mcp.example.com/mcp", secrets=["TOKEN"])
    expected = {"type": "http", "url": spec.url}
    assert connector_render.mcp_entry("acme", "acme-bot", "acme", "docs", spec) == expected
    assert connector_render.unhosted_mcp_entry(spec) == expected
    assert spec.secret_names() == spec.resolved_secrets() == ["TOKEN"]
    assert (
        connector_render.render(
            release="acme",
            agent="acme-bot",
            namespace="acme",
            app_name="curie",
            connector="docs",
            spec=spec,
            secret_name="acme-secrets",
        )
        == []
    )


@pytest.mark.parametrize("shared", [False, True])
def test_bundle_refuses_only_unsupported_shared_activation(tmp_path: Path, shared: bool) -> None:
    manifest = tmp_path / ".claude-plugin"
    manifest.mkdir()
    (manifest / "plugin.json").write_text(json.dumps({"name": "acme", "version": "0.1.0"}))
    declarations = {"dedicated": {"image": "example.com/acme/mcp:1"}}
    if shared:
        declarations["docs"] = {"shared": REFERENCE}
    (tmp_path / "connectors.yaml").write_text(json.dumps({"connectors": declarations}))
    result = validate_bundle(tmp_path)
    assert result.valid is not shared
    if shared:
        assert {issue.code.rsplit(".", 1)[-1] for issue in result.errors} == {
            "shared_connector_requires_application"
        }
