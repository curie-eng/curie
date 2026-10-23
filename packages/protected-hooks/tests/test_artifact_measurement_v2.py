"""Closed v2 measurements are structural claims, never runtime qualification."""

from __future__ import annotations

import copy
import hashlib
from dataclasses import FrozenInstanceError
from typing import Any

import pytest
from curie_protected_hooks import artifact_measurement as am
from curie_protected_hooks.authority_records import parse_manifest, parse_qualification

from .test_artifact_measurement import encoded, examples


def value() -> dict[str, Any]:
    """Synthetic upstream fixture, with v2 requirements from Accepted ADR0245."""
    result = examples()[2]
    result["schema_version"] = 2
    result["substrate"]["kind"] = "kubernetes"
    result["launch_projection"].update(schema_version=2, format="kubernetes-custodied-pod-v1")
    result["platform_mappings"][0]["runtime_profile"] = "protected-sdk-custody-v1"
    result["delivery_slots"].update(
        schema_version=2,
        lifecycle_context="guarded-lifecycle-context-v1",
        sdk_lifecycle="owned-cold-sdk-capsule-v1",
    )
    return result


def parser() -> Any:
    candidate = getattr(am, "parse_artifact_measurement_v2", None)
    assert callable(candidate), "distinct v2 artifact parser is missing"
    return candidate


def test_v2_canonical_bytes_are_immutable_and_do_not_select_v1_authority() -> None:
    manifest, qualification, _ = examples()
    manifest["substrate"]["kind"] = "kubernetes"
    data = value()
    data["manifest_digest"] = hashlib.sha256(encoded(manifest)).hexdigest()
    raw = encoded(data)
    qualification["manifest_digest"] = data["manifest_digest"]
    qualification["measurement_record_id"] = "sha256:" + hashlib.sha256(raw).hexdigest()
    result = parser()(raw)
    assert type(result) is am.ArtifactMeasurementV2
    assert result.canonical_bytes == raw
    assert result.digest == hashlib.sha256(raw).hexdigest()
    observed = result.as_dict()
    observed["delivery_slots"].clear()
    assert result.as_dict() == data
    with pytest.raises((FrozenInstanceError, AttributeError)):
        result._canonical_bytes = b"{}"
    with pytest.raises(am.ArtifactMeasurementInvalid):
        am.parse_artifact_measurement(raw)
    with pytest.raises(am.ArtifactMeasurementInvalid):
        parser()(encoded(examples()[2]))
    with pytest.raises(am.ArtifactMeasurementInvalid):
        am.validate_artifact_selection(
            result, parse_manifest(encoded(manifest)), parse_qualification(encoded(qualification))
        )


@pytest.mark.parametrize(
    "path",
    [
        ("schema_version",),
        ("delivery_slots", "schema_version"),
        ("launch_projection", "schema_version"),
    ],
)
@pytest.mark.parametrize("version", [1, 3, True, 2.0, "2", None])
def test_every_nested_version_is_exactly_integer_two(path: tuple[str, ...], version: Any) -> None:
    data = value()
    target = data
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = version
    with pytest.raises(am.ArtifactMeasurementInvalid):
        parser()(encoded(data))


@pytest.mark.parametrize(
    "mutation",
    [
        "missing-context",
        "extra-slot",
        "old-context",
        "old-sdk",
        "old-slot",
        "old-format",
        "projection-hash",
        "profile",
        "os",
        "substrate",
        "image",
        "mapping-extra",
        "mapping-duplicate",
        "guard",
        "top-extra",
        "rows-empty",
    ],
)
def test_v2_keeps_closed_delivery_and_original_measurement_constraints(mutation: str) -> None:
    data = value()
    slots, row = data["delivery_slots"], data["platform_mappings"][0]
    if mutation == "missing-context":
        del slots["lifecycle_context"]
    elif mutation == "extra-slot":
        slots["unknown"] = "unknown"
    elif mutation == "old-context":
        slots["lifecycle_context"] = "guarded-lifecycle-context-v0"
    elif mutation == "old-sdk":
        slots["sdk_lifecycle"] = "owned-cold-sdk-capsule-v0"
    elif mutation == "old-slot":
        slots["runner_token"] = "arbitrary"
    elif mutation == "old-format":
        data["launch_projection"]["format"] = "kubernetes-pod-v1"
    elif mutation == "projection-hash":
        data["launch_projection"]["sha256"] = "a" * 64
    elif mutation == "profile":
        row["runtime_profile"] = "generic"
    elif mutation == "os":
        row["os"] = "darwin"
    elif mutation == "substrate":
        data["substrate"]["kind"] = "docker"
    elif mutation == "image":
        row["oci_digest"] = "sha256:" + "a" * 64
    elif mutation == "mapping-extra":
        row["unknown"] = True
    elif mutation == "mapping-duplicate":
        data["platform_mappings"].append(copy.deepcopy(row))
    elif mutation == "guard":
        data["guard_identity"]["revision"] = 1
    elif mutation == "top-extra":
        data["unknown"] = True
    elif mutation == "rows-empty":
        data["platform_mappings"] = []
    with pytest.raises(am.ArtifactMeasurementInvalid):
        parser()(encoded(data))


@pytest.mark.parametrize("bad", [b"{}", b"null", b"[]", b"\xff", b"NaN", b"1.0", b"x" * 262145])
def test_invalid_input_has_fixed_detached_errors(bad: bytes) -> None:
    with pytest.raises(am.ArtifactMeasurementInvalid) as err:
        parser()(bad)
    assert str(err.value) == "Invalid protected artifact measurement"
    assert err.value.__cause__ is None and err.value.__context__ is None


def test_noncanonical_duplicate_and_mutable_bytes_refuse() -> None:
    raw = encoded(value())
    for bad in [b" " + raw, raw + b"\n", b'{"schema_version":2,' + raw[1:], bytearray(raw)]:
        with pytest.raises(am.ArtifactMeasurementInvalid):
            parser()(bad)
    cls = getattr(am, "ArtifactMeasurementV2", None)
    assert cls is not None
    with pytest.raises(am.ArtifactMeasurementInvalid):
        cls(b"{}")
