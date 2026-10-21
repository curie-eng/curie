"""Closed commissioning bootstrap grammar, @spec PROTECTED-COMMISSIONING-2."""

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


def encoded(value: Any) -> bytes:
    """Independent canonical byte oracle, @spec PROTECTED-COMMISSIONING-2."""
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")


def bootstrap_value() -> dict[str, Any]:
    """Anonymous bootstrap B bound to the example manifest, @spec PROTECTED-COMMISSIONING-2."""
    m = example_records()["manifest"]
    return {
        "schema_version": 1,
        "record_type": "commissioning_bootstrap",
        "runtime_id": m["runtime_id"],
        "runtime_generation": m["runtime_generation"],
        "execution_config_digest": m["execution_config_digest"],
        "entry_points": {
            "production": "composition/example-production",
            "commissioning": "composition/example-commissioning",
        },
        "production_credentials": copy.deepcopy(m["credential_refs"]),
        "candidate_credentials": {
            "enqueue": {"id": "commissioning/example-enqueue", "generation": "1"},
            "worker": {"id": "commissioning/example-worker", "generation": "2"},
        },
        "issuer_identity": {"id": "commissioning/example-issuer", "generation": "1"},
        "control_identity": {"id": "commissioning/example-control", "generation": "1"},
        "qualification_publisher_identity": {
            "id": "commissioning/example-qualification-publisher",
            "generation": "1",
        },
        "mediator_identity": {"id": "commissioning/example-mediator", "generation": "1"},
        "gateway_identity": {"id": "commissioning/example-gateway", "generation": "1"},
        "source_scope": [
            {
                "source_id": "77777777-7777-4777-8777-777777777777",
                "generation": "1",
                "policy_fingerprint": "8" * 64,
            },
            {
                "source_id": "88888888-8888-4888-8888-888888888888",
                "generation": "2",
                "policy_fingerprint": "9" * 64,
            },
        ],
        "target_scope": ["fixture/example-readonly", "fixture/example-second"],
        "max_permit_ms": "1000",
    }


def module() -> Any:
    name = "curie_protected_hooks.commissioning_bootstrap"
    assert importlib.util.find_spec(name) is not None, "commissioning bootstrap not implemented"
    return importlib.import_module(name)


def parse(value: Any) -> Any:
    return module().parse_commissioning_bootstrap(encoded(value))


def object_paths(value: Any, prefix: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    result = [prefix] if type(value) is dict else []
    if type(value) in (dict, list):
        children = value.items() if type(value) is dict else enumerate(value)
        for key, child in children:
            result.extend(object_paths(child, (*prefix, key)))
    return result


def at(value: Any, path: tuple[Any, ...]) -> Any:
    for key in path:
        value = value[key]
    return value


def test_canonical_bootstrap_is_immutable_and_order_invariant() -> None:
    api = module()
    value = bootstrap_value()
    raw = encoded(value)
    reordered = json.dumps(dict(reversed(list(value.items()))), indent=3).encode()
    record = api.parse_commissioning_bootstrap(reordered)
    assert record.canonical_bytes == raw
    assert record.digest == hashlib.sha256(raw).hexdigest()
    assert api.parse_commissioning_bootstrap(raw).digest == record.digest
    assert record.as_dict() == value
    record.as_dict()["source_scope"][0]["generation"] = "99"
    record.as_dict()["entry_points"]["commissioning"] = "changed"
    assert record.canonical_bytes == raw
    with pytest.raises(FrozenInstanceError):
        record._canonical_bytes = b"{}"
    assert "commissioning/example" not in repr(record)


@pytest.mark.parametrize(
    "field", ["manifest_digest", "guard_config_digest", "guard_identity", "permit", "files"]
)
def test_bootstrap_cannot_carry_records_whose_hash_includes_it(field: str) -> None:
    """Hash order step 1: B holds no M, G or permit, @spec PROTECTED-COMMISSIONING-2."""
    api = module()
    with pytest.raises(api.CommissioningRecordInvalid):
        parse(bootstrap_value() | {field: "a" * 64})


@pytest.mark.parametrize("path", object_paths(bootstrap_value()))
def test_every_nested_bootstrap_object_is_closed(path: tuple[Any, ...]) -> None:
    api = module()
    for key in at(bootstrap_value(), path):
        for replacement in ("missing", None, True, 1.0, 0):
            value = bootstrap_value()
            obj = at(value, path)
            if replacement == "missing":
                del obj[key]
            else:
                obj[key] = replacement
            if encoded(value) == encoded(bootstrap_value()):
                continue  # the replacement equals a valid original, such as database 0
            with pytest.raises(api.CommissioningRecordInvalid):
                parse(value)
    value = bootstrap_value()
    at(value, path)["unexpected"] = "private-untrusted-marker"
    with pytest.raises(api.CommissioningRecordInvalid) as error:
        parse(value)
    assert "private-untrusted-marker" not in str(error.value)


@pytest.mark.parametrize(
    "mutation",
    [
        # Entry points are opaque references, never importable module names.
        lambda v: v["entry_points"].update(commissioning=v["entry_points"]["production"]),
        lambda v: v["entry_points"].update(production="curie worker.protected_entry"),
        lambda v: v.update(record_type="commissioning_permit"),
        lambda v: v.update(schema_version=2),
        lambda v: v.update(schema_version=True),
        lambda v: v.update(max_permit_ms="0"),
        lambda v: v.update(max_permit_ms=1000),
        lambda v: v["candidate_credentials"].update(worker=v["candidate_credentials"]["enqueue"]),
        lambda v: v["candidate_credentials"]["worker"].update(
            id=v["production_credentials"]["worker"]["id"], generation="999"
        ),
        lambda v: v["issuer_identity"].update(id=v["control_identity"]["id"]),
        lambda v: v["control_identity"].update(id=v["candidate_credentials"]["enqueue"]["id"]),
        lambda v: v["issuer_identity"].update(id=v["production_credentials"]["verifier"]["id"]),
        # Design decision 8: the Qualification publisher is its own broker identity.
        lambda v: v["qualification_publisher_identity"].update(id=v["issuer_identity"]["id"]),
        lambda v: v["qualification_publisher_identity"].update(id=v["control_identity"]["id"]),
        lambda v: v["qualification_publisher_identity"].update(
            id=v["candidate_credentials"]["worker"]["id"]
        ),
        lambda v: v["qualification_publisher_identity"].update(
            id=v["production_credentials"]["verifier"]["id"], generation="7"
        ),
        lambda v: v["qualification_publisher_identity"].update(id=v["mediator_identity"]["id"]),
        lambda v: v["qualification_publisher_identity"].update(id=v["gateway_identity"]["id"]),
        # Every credential ID in B is pairwise distinct, launch identities included.
        lambda v: v["mediator_identity"].update(id=v["gateway_identity"]["id"]),
        lambda v: v["mediator_identity"].update(id=v["issuer_identity"]["id"]),
        lambda v: v["gateway_identity"].update(id=v["production_credentials"]["worker"]["id"]),
        lambda v: v["production_credentials"]["worker"].update(
            id=v["production_credentials"]["enqueue"]["id"]
        ),
    ],
)
def test_bootstrap_refuses_selector_or_credential_substitution(mutation: Any) -> None:
    api = module()
    value = bootstrap_value()
    mutation(value)
    with pytest.raises(api.CommissioningRecordInvalid):
        parse(value)


@pytest.mark.parametrize("field", ["source_scope", "target_scope"])
@pytest.mark.parametrize("kind", ["empty", "duplicate", "too_many", "unsorted", "not_array"])
def test_bootstrap_scope_is_bounded_unique_and_sorted(field: str, kind: str) -> None:
    api = module()
    value = bootstrap_value()
    first = value[field][0]
    if kind == "empty":
        value[field] = []
    elif kind == "duplicate":
        value[field] = [first, first]
    elif kind == "too_many":
        value[field] = [first] * 65
    elif kind == "unsorted":
        value[field] = list(reversed(value[field]))
    else:
        value[field] = first
    with pytest.raises(api.CommissioningRecordInvalid):
        parse(value)


def test_bootstrap_source_ids_are_distinct_even_with_different_generations() -> None:
    api = module()
    value = bootstrap_value()
    second = copy.deepcopy(value["source_scope"][0])
    second["generation"] = "5"
    value["source_scope"] = [value["source_scope"][0], second]
    with pytest.raises(api.CommissioningRecordInvalid):
        parse(value)


def test_bootstrap_scope_accepts_sixty_four_members() -> None:
    value = bootstrap_value()
    value["target_scope"] = sorted(f"fixture/example-{index:02d}" for index in range(64))
    value["source_scope"] = [
        {
            "source_id": f"{index:08x}-0000-4000-8000-000000000000",
            "generation": "1",
            "policy_fingerprint": "8" * 64,
        }
        for index in range(1, 65)
    ]
    assert parse(value).as_dict() == value


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
def test_bootstrap_invalid_encoding_refuses(raw: bytes) -> None:
    api = module()
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_commissioning_bootstrap(raw)


@pytest.mark.parametrize("raw", [1, "{}", bytearray(b"{}"), None])
def test_bootstrap_refuses_non_bytes_input(raw: Any) -> None:
    api = module()
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_commissioning_bootstrap(raw)


@pytest.mark.parametrize("number", [b"1.0", b"1e3", b"Infinity", b"-Infinity", b"NaN"])
def test_bootstrap_refuses_float_and_nonfinite_numbers(number: bytes) -> None:
    api = module()
    raw = encoded(bootstrap_value()).replace(b'"schema_version":1', b'"schema_version":' + number)
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_commissioning_bootstrap(raw)


def test_bootstrap_byte_limit_is_16384() -> None:
    api = module()
    raw = encoded(bootstrap_value())
    padded = raw[:-1] + b" " * (16384 - len(raw)) + b"}"
    assert len(padded) == 16384
    assert api.parse_commissioning_bootstrap(padded).canonical_bytes == raw
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_commissioning_bootstrap(raw[:-1] + b" " * (16385 - len(raw)) + b"}")
