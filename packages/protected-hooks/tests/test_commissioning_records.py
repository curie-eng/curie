"""Closed commissioning permit grammar, @spec PROTECTED-COMMISSIONING-1."""

from __future__ import annotations

import copy
import hashlib
import importlib
import importlib.util
import json
from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from .test_authority_records import example_records
from .test_commissioning_bootstrap import at, bootstrap_value, encoded, object_paths


def permit_value() -> dict[str, Any]:
    """Anonymous permit bound to the example manifest and B, @spec PROTECTED-COMMISSIONING-1."""
    m = example_records()["manifest"]
    b = bootstrap_value()
    return {
        "schema_version": 1,
        "record_type": "commissioning_permit",
        "campaign_id": "66666666-6666-4666-8666-666666666666",
        "campaign_generation": "1",
        "runtime_id": m["runtime_id"],
        "runtime_generation": m["runtime_generation"],
        "manifest_digest": hashlib.sha256(encoded(m)).hexdigest(),
        "bootstrap_digest": hashlib.sha256(encoded(b)).hexdigest(),
        "execution_config_digest": m["execution_config_digest"],
        "broker_identity": copy.deepcopy(m["broker_identity"]),
        "guard_identity": copy.deepcopy(m["guard_identity"]),
        "issuer_identity": copy.deepcopy(b["issuer_identity"]),
        "control_identity": copy.deepcopy(b["control_identity"]),
        "candidate_credentials": copy.deepcopy(b["candidate_credentials"]),
        "sources": [copy.deepcopy(b["source_scope"][0])],
        "targets": [b["target_scope"][0]],
        "issued_at_ms": "1000",
        "expires_at_ms": "2000",
        "limits": {
            key: "4"
            for key in ("deliveries", "launches", "model_calls", "output_tokens", "spend_microusd")
        },
    }


def module() -> Any:
    name = "curie_protected_hooks.commissioning_records"
    assert importlib.util.find_spec(name) is not None, "commissioning permit codec not implemented"
    return importlib.import_module(name)


def parse(value: Any) -> Any:
    return module().parse_commissioning_permit(encoded(value))


def test_canonical_permit_is_immutable_and_order_invariant() -> None:
    api = module()
    value = permit_value()
    raw = encoded(value)
    reordered = json.dumps(dict(reversed(list(value.items()))), indent=2).encode()
    record = api.parse_commissioning_permit(reordered)
    assert record.canonical_bytes == raw
    assert record.digest == hashlib.sha256(raw).hexdigest()
    assert api.parse_commissioning_permit(raw).digest == record.digest
    assert record.as_dict() == value
    record.as_dict()["candidate_credentials"]["worker"]["id"] = "changed"
    record.as_dict()["sources"].clear()
    assert record.canonical_bytes == raw
    with pytest.raises(FrozenInstanceError):
        record._canonical_bytes = b"{}"
    assert "commissioning/example" not in repr(record)


@pytest.mark.parametrize("path", object_paths(permit_value()))
def test_every_nested_permit_object_is_closed(path: tuple[Any, ...]) -> None:
    api = module()
    for key in at(permit_value(), path):
        for replacement in ("missing", None, True, 1.0, 0):
            value = permit_value()
            obj = at(value, path)
            if replacement == "missing":
                del obj[key]
            else:
                obj[key] = replacement
            if encoded(value) == encoded(permit_value()):
                continue  # the replacement equals a valid original, such as database 0
            with pytest.raises(api.CommissioningRecordInvalid):
                parse(value)
    value = permit_value()
    at(value, path)["unexpected"] = "private-untrusted-marker"
    with pytest.raises(api.CommissioningRecordInvalid) as error:
        parse(value)
    assert "private-untrusted-marker" not in str(error.value)


@pytest.mark.parametrize("path", object_paths(permit_value()))
def test_duplicate_json_members_refuse_at_every_depth(path: tuple[Any, ...]) -> None:
    api = module()
    value = permit_value()
    obj = at(value, path)
    original = encoded(obj)
    first = next(iter(sorted(obj)))
    duplicate = b"{" + encoded(first) + b":" + encoded(obj[first]) + b"," + original[1:]
    raw = encoded(value).replace(original, duplicate, 1)
    assert raw != encoded(value)
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_commissioning_permit(raw)


@pytest.mark.parametrize("field", ["sources", "targets"])
@pytest.mark.parametrize("kind", ["empty", "duplicate", "too_many", "unsorted", "not_array"])
def test_permit_scope_is_bounded_unique_and_sorted(field: str, kind: str) -> None:
    api = module()
    value = permit_value()
    first = value[field][0]
    if kind == "empty":
        value[field] = []
    elif kind == "duplicate":
        value[field] = [first, first]
    elif kind == "too_many":
        value[field] = [first] * 65
    elif kind == "unsorted":
        b = bootstrap_value()
        value[field] = list(reversed(b["source_scope" if field == "sources" else "target_scope"]))
    else:
        value[field] = first
    with pytest.raises(api.CommissioningRecordInvalid):
        parse(value)


@pytest.mark.parametrize(
    "invalid", ["0", "01", "-1", "+1", "1e3", " 1", 1, True, "9223372036854775808"]
)
@pytest.mark.parametrize("field", ["campaign_generation", "deliveries", "spend_microusd"])
def test_counts_and_generations_are_generation_strings(field: str, invalid: Any) -> None:
    api = module()
    value = permit_value()
    (value if field == "campaign_generation" else value["limits"])[field] = invalid
    with pytest.raises(api.CommissioningRecordInvalid):
        parse(value)


@pytest.mark.parametrize(
    ("first", "second"),
    [
        (("issuer_identity",), ("control_identity",)),
        (("issuer_identity",), ("candidate_credentials", "enqueue")),
        (("issuer_identity",), ("candidate_credentials", "worker")),
        (("control_identity",), ("candidate_credentials", "enqueue")),
        (("control_identity",), ("candidate_credentials", "worker")),
        (("candidate_credentials", "enqueue"), ("candidate_credentials", "worker")),
    ],
)
def test_four_commissioning_credentials_are_pairwise_distinct(
    first: tuple[str, ...], second: tuple[str, ...]
) -> None:
    api = module()
    value = permit_value()
    at(value, second)["id"] = at(value, first)["id"]
    at(value, second)["generation"] = "77"
    with pytest.raises(api.CommissioningRecordInvalid):
        parse(value)


@pytest.mark.parametrize(("issued", "expires"), [("2000", "2000"), ("2001", "2000")])
def test_permit_interval_is_nonempty(issued: str, expires: str) -> None:
    api = module()
    value = permit_value()
    value.update(issued_at_ms=issued, expires_at_ms=expires)
    with pytest.raises(api.CommissioningRecordInvalid):
        parse(value)


def test_permit_millisecond_accepts_zero_and_refuses_past_safe_integer() -> None:
    api = module()
    value = permit_value()
    value["issued_at_ms"] = "0"
    assert parse(value).as_dict()["issued_at_ms"] == "0"
    value["expires_at_ms"] = "9007199254740992"
    with pytest.raises(api.CommissioningRecordInvalid):
        parse(value)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda v: v.update(record_type="commissioning_bootstrap"),
        lambda v: v.update(record_type="Commissioning_permit"),
        lambda v: v.update(schema_version=2),
        lambda v: v.update(campaign_id="66666666-6666-4666-8666-66666666666G"),
        lambda v: v.update(campaign_id="66666666666646668666666666666666"),
        lambda v: v.update(manifest_digest="A" * 64),
        lambda v: v["targets"].__setitem__(0, "-fixture"),
        lambda v: v["sources"][0].update(policy_fingerprint="8" * 63),
    ],
)
def test_permit_refuses_invalid_scalars(mutation: Any) -> None:
    api = module()
    value = permit_value()
    mutation(value)
    with pytest.raises(api.CommissioningRecordInvalid):
        parse(value)


@pytest.mark.parametrize(
    "raw",
    [
        b'{"schema_version":1,"schema_version":1}',
        b"\xff",
        b"{} {}",
        b"NaN",
        b"[]",
        b"",
    ],
)
def test_permit_invalid_encoding_refuses(raw: bytes) -> None:
    api = module()
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_commissioning_permit(raw)


@pytest.mark.parametrize("number", [b"1.0", b"1e3", b"Infinity", b"-Infinity", b"NaN"])
def test_permit_refuses_float_and_nonfinite_numbers(number: bytes) -> None:
    api = module()
    raw = encoded(permit_value()).replace(b'"schema_version":1', b'"schema_version":' + number)
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_commissioning_permit(raw)


def test_permit_byte_limit_is_16384() -> None:
    api = module()
    raw = encoded(permit_value())
    padded = raw[:-1] + b" " * (16384 - len(raw)) + b"}"
    assert len(padded) == 16384
    assert api.parse_commissioning_permit(padded).canonical_bytes == raw
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_commissioning_permit(raw[:-1] + b" " * (16385 - len(raw)) + b"}")


def selection_value() -> dict[str, Any]:
    """Valid production selection oracle, @spec PROTECTED-HOOK-ADMISSION-2."""
    m = example_records()["manifest"]
    return {
        "schema_version": 1,
        "runtime_id": m["runtime_id"],
        "runtime_generation": m["runtime_generation"],
        "manifest_digest": hashlib.sha256(encoded(m)).hexdigest(),
        "qualification_id": m["qualification_id"],
        "qualification_generation": "1",
        "broker_run_id": m["broker_identity"]["run_id"],
        "admission_open": True,
    }


def production_decoders() -> dict[str, Any]:
    from curie_protected_hooks.admission_records import parse_selection
    from curie_protected_hooks.authority_records import (
        parse_manifest,
        parse_qualification,
        parse_readiness,
    )

    return {
        "manifest": parse_manifest,
        "qualification": parse_qualification,
        "readiness": parse_readiness,
        "selection": parse_selection,
    }


def production_examples() -> dict[str, bytes]:
    records = example_records()
    return {
        "manifest": encoded(records["manifest"]),
        "qualification": encoded(records["qualification"]),
        "readiness": encoded(records["readiness"]),
        "selection": encoded(selection_value()),
    }


@pytest.mark.parametrize("decoder", ["manifest", "qualification", "readiness", "selection"])
@pytest.mark.parametrize("record", ["permit", "bootstrap"])
def test_production_decoders_refuse_commissioning_records(decoder: str, record: str) -> None:
    raw = encoded(permit_value() if record == "permit" else bootstrap_value())
    parse_production = production_decoders()[decoder]
    parse_production(production_examples()[decoder])
    with pytest.raises(ValueError):
        parse_production(raw)


@pytest.mark.parametrize("record", ["manifest", "qualification", "readiness", "selection"])
@pytest.mark.parametrize("parser", ["permit", "bootstrap"])
def test_commissioning_decoders_refuse_production_records(record: str, parser: str) -> None:
    from curie_protected_hooks import commissioning_bootstrap

    api = module()
    parse_commissioning = (
        api.parse_commissioning_permit
        if parser == "permit"
        else commissioning_bootstrap.parse_commissioning_bootstrap
    )
    with pytest.raises(api.CommissioningRecordInvalid):
        parse_commissioning(production_examples()[record])


def test_permit_and_bootstrap_parsers_refuse_each_other() -> None:
    from curie_protected_hooks import commissioning_bootstrap

    api = module()
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_commissioning_permit(encoded(bootstrap_value()))
    with pytest.raises(api.CommissioningRecordInvalid):
        commissioning_bootstrap.parse_commissioning_bootstrap(encoded(permit_value()))
    assert commissioning_bootstrap.CommissioningRecordInvalid is api.CommissioningRecordInvalid
