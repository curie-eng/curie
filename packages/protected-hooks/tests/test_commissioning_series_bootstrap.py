"""Explicit B v2 stays separate from inert v1, @spec COMMISSIONING-SERIES-3."""

from __future__ import annotations

import copy
import hashlib
import importlib
import importlib.util
import json
from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from .test_commissioning_bootstrap import at, bootstrap_value, encoded, object_paths


def series_bootstrap_value() -> dict[str, Any]:
    """Independent anonymous v2 fixture; no runtime authority."""
    return bootstrap_value() | {
        "schema_version": 2,
        "evidence_observer_identity": {"id": "commissioning/example-evidence", "generation": "1"},
    }


def module() -> Any:
    name = "curie_protected_hooks.commissioning_series_bootstrap"
    assert importlib.util.find_spec(name) is not None, "series bootstrap v2 not implemented"
    return importlib.import_module(name)


def test_series_bootstrap_canonical_roundtrip_and_immutable_copies() -> None:
    api = module()
    value = series_bootstrap_value()
    raw = encoded(value)
    record = api.parse_series_bootstrap(json.dumps(value, indent=2).encode())
    assert record.canonical_bytes == raw
    assert record.digest == hashlib.sha256(raw).hexdigest()
    assert record.as_dict() == value
    record.as_dict()["evidence_observer_identity"]["id"] = "changed"
    assert record.as_dict() == value
    assert record.canonical_bytes == raw
    with pytest.raises(FrozenInstanceError):
        record._canonical_bytes = b"{}"
    assert "example-evidence" not in repr(record)


@pytest.mark.parametrize("version", [1, True, 2.0, "2", 3, None])
def test_only_explicit_integer_version_two_is_accepted(version: Any) -> None:
    api = module()
    value = series_bootstrap_value() | {"schema_version": version}
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_series_bootstrap(encoded(value))


def test_version_one_and_two_readers_never_fall_back() -> None:
    from curie_protected_hooks.commissioning_bootstrap import parse_commissioning_bootstrap
    from curie_protected_hooks.commissioning_encoding import CommissioningRecordInvalid

    api = module()
    with pytest.raises(CommissioningRecordInvalid):
        parse_commissioning_bootstrap(encoded(series_bootstrap_value()))
    with pytest.raises(CommissioningRecordInvalid):
        api.parse_series_bootstrap(encoded(bootstrap_value()))


@pytest.mark.parametrize("path", object_paths(series_bootstrap_value()))
def test_each_series_bootstrap_object_rejects_extra_and_missing_members(
    path: tuple[Any, ...],
) -> None:
    api = module()
    for key in at(series_bootstrap_value(), path):
        value = series_bootstrap_value()
        del at(value, path)[key]
        with pytest.raises(api.CommissioningRecordInvalid):
            api.parse_series_bootstrap(encoded(value))
    value = series_bootstrap_value()
    at(value, path)["unexpected"] = "untrusted-private-marker"
    with pytest.raises(api.CommissioningRecordInvalid) as error:
        api.parse_series_bootstrap(encoded(value))
    assert "untrusted-private-marker" not in str(error.value)


def identity_references() -> list[dict[str, str]]:
    value = bootstrap_value()
    return [
        *value["production_credentials"].values(),
        *value["candidate_credentials"].values(),
        *(
            value[key]
            for key in (
                "issuer_identity",
                "control_identity",
                "qualification_publisher_identity",
                "control_reader_identity",
                "source_writer_identity",
                "mediator_identity",
                "gateway_identity",
            )
        ),
    ]


@pytest.mark.parametrize("identity", identity_references())
def test_evidence_identity_cannot_alias_an_existing_issuance(identity: dict[str, str]) -> None:
    api = module()
    value = series_bootstrap_value()
    value["evidence_observer_identity"] = identity | {"generation": "99"}
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_series_bootstrap(encoded(value))


def test_existing_identity_and_entry_point_separation_is_preserved() -> None:
    api = module()
    value = series_bootstrap_value()
    value["gateway_identity"] = copy.deepcopy(value["mediator_identity"])
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_series_bootstrap(encoded(value))
    value = series_bootstrap_value()
    value["entry_points"]["commissioning"] = value["entry_points"]["production"]
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_series_bootstrap(encoded(value))


@pytest.mark.parametrize(
    "raw",
    [
        b"{}",
        b"[]",
        b"null",
        b"\xff",
        b"{" + b" " * 16384 + b"}",
        b'{"schema_version":2,"schema_version":2}',
    ],
)
def test_invalid_or_oversized_input_refuses_without_echo(raw: bytes) -> None:
    api = module()
    with pytest.raises(api.CommissioningRecordInvalid):
        api.SeriesBootstrap(raw)


def test_duplicate_nested_identity_member_is_not_normalized() -> None:
    api = module()
    raw = encoded(series_bootstrap_value()).replace(
        b'"id":"commissioning/example-evidence"',
        b'"id":"untrusted-private-marker","id":"commissioning/example-evidence"',
    )
    with pytest.raises(api.CommissioningRecordInvalid) as error:
        api.parse_series_bootstrap(raw)
    assert "untrusted-private-marker" not in str(error.value)
