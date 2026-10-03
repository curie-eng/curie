"""Closed internal authority contracts, @spec PROTECTED-HOOK-LANE-2."""

from __future__ import annotations

import copy
import hashlib
import importlib
import importlib.util
import json
from itertools import combinations
from typing import Any

import pytest


def example_records() -> dict[str, dict[str, Any]]:
    """Anonymous independent contract examples, @spec PROTECTED-HOOK-LANE-2."""
    broker = {
        "instance_id": "11111111-1111-4111-8111-111111111111",
        "endpoint": {"host": "broker.example.test", "port": 6380},
        "tls_server_name": "broker.example.test",
        "tls_spki_sha256": "a" * 64,
        "run_id": "b" * 40,
        "database": 0,
    }
    guard = {
        "control_id": "22222222-2222-4222-8222-222222222222",
        "revision": "3",
        "config_sha256": "c" * 64,
    }
    manifest = {
        "schema_version": 1,
        "runtime_id": "33333333-3333-4333-8333-333333333333",
        "runtime_generation": "9007199254740993",
        "broker_identity": broker,
        "worker_image_digest": "sha256:" + "d" * 64,
        "runner_image_digest": "sha256:" + "e" * 64,
        "bundle_digest": {"sha256": "f" * 64, "object_identity": "bundle/example-v1"},
        "execution_config_digest": "1" * 64,
        "qualification_id": "44444444-4444-4444-8444-444444444444",
        "substrate": {
            "kind": "docker",
            "authority_domain_id": "55555555-5555-4555-8555-555555555555",
            "launch_identity": "launch/example-v1",
            "launch_config_sha256": "2" * 64,
        },
        "guard_identity": guard,
        "credential_refs": {
            "enqueue": {"id": "credential/example-enqueue", "generation": "4"},
            "worker": {"id": "credential/example-worker", "generation": "5"},
            "verifier": {"id": "credential/example-verifier", "generation": "6"},
        },
    }
    qualification = {
        "schema_version": 1,
        "qualification_id": manifest["qualification_id"],
        "qualification_generation": "7",
        "runtime_id": manifest["runtime_id"],
        "runtime_generation": manifest["runtime_generation"],
        "manifest_digest": hashlib.sha256(encoded(manifest)).hexdigest(),
        "broker_identity": copy.deepcopy(broker),
        "execution_config_digest": manifest["execution_config_digest"],
        "guard_identity": copy.deepcopy(guard),
        "measurement_record_id": "measurement/example-qualification",
    }
    readiness = {
        **{
            key: copy.deepcopy(qualification[key])
            for key in (
                "schema_version",
                "manifest_digest",
                "runtime_id",
                "runtime_generation",
                "qualification_id",
                "qualification_generation",
                "broker_identity",
                "guard_identity",
            )
        },
        "verifier_identity": copy.deepcopy(manifest["credential_refs"]["verifier"]),
        "issued_at_ms": "1000",
        "expires_at_ms": "2000",
        "measurement_record_id": "measurement/example-readiness",
    }
    return {"manifest": manifest, "qualification": qualification, "readiness": readiness}


def encoded(value: Any) -> bytes:
    """Specification byte oracle, @spec PROTECTED-HOOK-LANE-2."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "ascii"
    )


def object_paths(value: dict[str, Any], prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    """Enumerate specified closed objects, @spec PROTECTED-HOOK-LANE-2."""
    paths = [prefix]
    for key, child in value.items():
        if isinstance(child, dict):
            paths.extend(object_paths(child, (*prefix, key)))
    return paths


def at(value: dict[str, Any], path: tuple[str, ...]) -> Any:
    """Navigate an independent example, @spec PROTECTED-HOOK-LANE-2."""
    current: Any = value
    for key in path:
        current = current[key]
    return current


def altered(kind: str, path: tuple[str, ...], value: Any) -> dict[str, Any]:
    """Produce one violating input, @spec PROTECTED-HOOK-LANE-2."""
    record = example_records()[kind]
    at(record, path[:-1])[path[-1]] = value
    return record


def authority_module() -> Any:
    """Require the new feature without a collection error, @spec PROTECTED-HOOK-LANE-2."""
    assert importlib.util.find_spec("curie_protected_hooks.authority_records") is not None, (
        "closed protected authority records are not implemented"
    )
    return importlib.import_module("curie_protected_hooks.authority_records")


def parse(authority: Any, kind: str, value: Any) -> Any:
    """Call the intended public internal seam, @spec PROTECTED-HOOK-LANE-2."""
    return getattr(authority, "parse_" + kind)(
        value if isinstance(value, bytes) else encoded(value)
    )


CLOSED_OBJECTS = [
    (kind, path) for kind, value in example_records().items() for path in object_paths(value)
]
REQUIRED_FIELDS = [
    (kind, (*path, field))
    for kind, path in CLOSED_OBJECTS
    for field in at(example_records()[kind], path)
]
GENERATION_FIELDS = [
    (kind, path)
    for kind, path in REQUIRED_FIELDS
    if path[-1] in {"runtime_generation", "qualification_generation", "generation", "revision"}
]


@pytest.mark.parametrize("kind", ["manifest", "qualification", "readiness"])
def test_valid_records_have_exact_canonical_bytes_and_sha256(kind: str) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    data = example_records()[kind]
    record = parse(authority, kind, data)
    assert record.as_dict() == data
    assert record.canonical_bytes == encoded(data)
    assert record.digest == hashlib.sha256(encoded(data)).hexdigest()
    reordered = json.dumps(dict(reversed(list(data.items()))), indent=2).encode()
    assert parse(authority, kind, reordered).canonical_bytes == record.canonical_bytes


@pytest.mark.parametrize("kind,path", REQUIRED_FIELDS)
@pytest.mark.parametrize("violation", ["missing", "null"])
def test_every_nested_field_is_required_non_null(
    kind: str, path: tuple[str, ...], violation: str
) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    data = example_records()[kind]
    parent = at(data, path[:-1])
    if violation == "missing":
        del parent[path[-1]]
    else:
        parent[path[-1]] = None
    with pytest.raises(authority.AuthorityRecordInvalid):
        parse(authority, kind, data)


@pytest.mark.parametrize("kind,path", CLOSED_OBJECTS)
def test_extra_fields_are_refused_at_every_depth(kind: str, path: tuple[str, ...]) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    data = example_records()[kind]
    at(data, path)["unknown"] = "example-marker"
    with pytest.raises(authority.AuthorityRecordInvalid):
        parse(authority, kind, data)


@pytest.mark.parametrize(
    "kind,path",
    [
        (kind, path)
        for kind, path in REQUIRED_FIELDS
        if not isinstance(at(example_records()[kind], path), dict)
    ],
)
@pytest.mark.parametrize("invalid", [True, 1.25])
def test_all_nested_scalars_reject_boolean_and_float_coercion(
    kind: str,
    path: tuple[str, ...],
    invalid: Any,
) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    with pytest.raises(authority.AuthorityRecordInvalid):
        parse(authority, kind, altered(kind, path, invalid))


@pytest.mark.parametrize("kind,path", CLOSED_OBJECTS)
def test_duplicate_members_are_refused_recursively(kind: str, path: tuple[str, ...]) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    data = example_records()[kind]

    def duplicate_json(node: Any, current: tuple[str, ...] = ()) -> str:
        """Make identical duplicate members, @spec PROTECTED-HOOK-LANE-2."""
        if not isinstance(node, dict):
            return json.dumps(node)
        members = [
            json.dumps(key) + ":" + duplicate_json(child, (*current, key))
            for key, child in node.items()
        ]
        if current == path:
            members.append(members[0])
        return "{" + ",".join(members) + "}"

    with pytest.raises(authority.AuthorityRecordInvalid):
        parse(authority, kind, duplicate_json(data).encode())


@pytest.mark.parametrize("kind", ["manifest", "qualification", "readiness"])
@pytest.mark.parametrize(
    "raw", [b"\xff", b"{} {}", b"[]", b"null", b"NaN", b"Infinity", b"-Infinity", b"", b" " * 16385]
)
def test_invalid_or_oversized_json_refuses(kind: str, raw: bytes) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    with pytest.raises(authority.AuthorityRecordInvalid):
        parse(authority, kind, raw)


@pytest.mark.parametrize("kind,path", GENERATION_FIELDS)
@pytest.mark.parametrize(
    "invalid", [0, 1, True, 1.0, "0", "01", "-1", "+1", "1e2", " 1", "9223372036854775808"]
)
def test_generations_are_exact_positive_bigint_strings(
    kind: str, path: tuple[str, ...], invalid: Any
) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    with pytest.raises(authority.AuthorityRecordInvalid):
        parse(authority, kind, altered(kind, path, invalid))


@pytest.mark.parametrize("kind,path", GENERATION_FIELDS)
@pytest.mark.parametrize("generation", ["1", "9007199254740993", "9223372036854775807"])
def test_generations_round_trip_without_lua_precision_loss(
    kind: str, path: tuple[str, ...], generation: str
) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    assert at(parse(authority, kind, altered(kind, path, generation)).as_dict(), path) == generation


@pytest.mark.parametrize(
    "path,invalid",
    [
        (("schema_version",), True),
        (("schema_version",), "1"),
        (("schema_version",), 1.0),
        (("schema_version",), 2),
        (("broker_identity", "database"), False),
        (("broker_identity", "database"), 1),
        (("broker_identity", "database"), "0"),
        (("broker_identity", "database"), 0.0),
        (("broker_identity", "endpoint", "port"), True),
        (("broker_identity", "endpoint", "port"), "6380"),
        (("broker_identity", "endpoint", "port"), 6380.0),
        (("broker_identity", "endpoint", "port"), 0),
        (("broker_identity", "endpoint", "port"), 65536),
        (("runtime_id",), "33333333333343338333333333333333"),
        (("qualification_id",), "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"),
        (("worker_image_digest",), "latest"),
        (("runner_image_digest",), "sha256:" + "A" * 64),
        (("execution_config_digest",), "a" * 63),
        (("bundle_digest", "sha256"), "sha256:" + "a" * 64),
        (("broker_identity", "run_id"), "b" * 39),
        (("broker_identity", "run_id"), "B" * 40),
        (("broker_identity", "tls_spki_sha256"), "A" * 64),
        (("substrate", "kind"), "compose"),
        (("substrate", "authority_domain_id"), "not-a-uuid"),
        (("substrate", "launch_identity"), ""),
        (("substrate", "launch_identity"), "x" * 257),
        (("bundle_digest", "object_identity"), "example reference"),
        (("credential_refs", "enqueue", "id"), "example\nmarker"),
        (("credential_refs", "worker", "id"), "예시"),
    ],
)
def test_manifest_refuses_noncanonical_or_coerced_scalar(
    path: tuple[str, ...], invalid: Any
) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    with pytest.raises(authority.AuthorityRecordInvalid):
        parse(authority, "manifest", altered("manifest", path, invalid))


@pytest.mark.parametrize("field", ["endpoint", "tls_server_name"])
@pytest.mark.parametrize(
    "invalid",
    [
        "Broker.example.test",
        "broker.example.test.",
        "-broker.example.test",
        "broker..example.test",
        "rediss://broker.example.test",
        "user:example@broker.example.test",
        "broker.example.test/path",
        "broker.example.test?x=1",
        "[::1]",
        "2001:0db8::1",
        "127.000.0.1",
        "999.999.999.999",
        "éxample.test",
        "a" * 64 + ".test",
    ],
)
def test_server_identity_host_is_canonical(field: str, invalid: str) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    path = (
        ("broker_identity", "endpoint", "host")
        if field == "endpoint"
        else ("broker_identity", field)
    )
    with pytest.raises(authority.AuthorityRecordInvalid):
        parse(authority, "manifest", altered("manifest", path, invalid))


@pytest.mark.parametrize("host", ["broker.example.test", "127.0.0.1", "::1", "2001:db8::1"])
@pytest.mark.parametrize("substrate", ["docker", "kubernetes"])
def test_canonical_endpoint_and_both_substrates_are_supported(host: str, substrate: str) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    data = example_records()["manifest"]
    data["broker_identity"]["endpoint"]["host"] = host
    data["broker_identity"]["tls_server_name"] = host
    data["substrate"]["kind"] = substrate
    assert parse(authority, "manifest", data).as_dict() == data


@pytest.mark.parametrize("left,right", list(combinations(["enqueue", "worker", "verifier"], 2)))
def test_credential_reference_ids_are_distinct_even_across_generations(
    left: str, right: str
) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    data = example_records()["manifest"]
    data["credential_refs"][right]["id"] = data["credential_refs"][left]["id"]
    with pytest.raises(authority.AuthorityRecordInvalid):
        parse(authority, "manifest", data)


@pytest.mark.parametrize("kind", ["manifest", "qualification", "readiness"])
def test_record_and_returned_nested_copies_cannot_mutate_identity(kind: str) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    data = example_records()[kind]
    record = parse(authority, kind, data)
    before = (record.canonical_bytes, record.digest)
    copy_out = record.as_dict()
    copy_out["broker_identity"]["endpoint"]["host"] = "other.example.test"
    copy_out["runtime_generation"] = "99"
    assert record.as_dict() == data
    assert (record.canonical_bytes, record.digest) == before
    with pytest.raises((AttributeError, TypeError)):
        record.digest = "0" * 64
    with pytest.raises((AttributeError, TypeError)):
        record.canonical_bytes = b"{}"


@pytest.mark.parametrize("field", ["issued_at_ms", "expires_at_ms"])
@pytest.mark.parametrize("invalid", [True, 1000, 1000.0, "01", "-1", "1e3", "9007199254740992"])
def test_readiness_timestamp_type_and_safe_integer_bounds(field: str, invalid: Any) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    with pytest.raises(authority.AuthorityRecordInvalid):
        parse(authority, "readiness", altered("readiness", (field,), invalid))


@pytest.mark.parametrize("issued,expires", [("1000", "1000"), ("2000", "1000")])
def test_readiness_invalid_order_is_structurally_refused(issued: str, expires: str) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    data = example_records()["readiness"]
    data.update(issued_at_ms=issued, expires_at_ms=expires)
    with pytest.raises(authority.AuthorityRecordInvalid):
        parse(authority, "readiness", data)


def check(authority: Any, records: dict[str, dict[str, Any]], **overrides: Any) -> None:
    """Pure matching seam with trusted observed facts, @spec PROTECTED-HOOK-LANE-2."""
    options = {
        "broker_identity": copy.deepcopy(records["manifest"]["broker_identity"]),
        "broker_now_ms": 1500,
        "trusted_max_readiness_ms": 1000,
        **overrides,
    }
    authority.validate_authority(
        *(
            parse(authority, kind, records[kind])
            for kind in ("manifest", "qualification", "readiness")
        ),
        **options,
    )


@pytest.mark.parametrize("now", [1000, 1500, 1999])
def test_matching_authority_accepts_bound_interval_and_distinct_measurements(now: int) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    check(authority, example_records(), broker_now_ms=now)


@pytest.mark.parametrize(
    "overrides",
    [
        {"broker_now_ms": 999},
        {"broker_now_ms": 2000},
        {"trusted_max_readiness_ms": 999},
        {"broker_now_ms": True},
        {"broker_now_ms": "1500"},
        {"broker_now_ms": 1500.0},
        {"broker_now_ms": -1},
        {"broker_now_ms": 9007199254740992},
        {"trusted_max_readiness_ms": True},
        {"trusted_max_readiness_ms": 0},
        {"trusted_max_readiness_ms": "1000"},
        {"trusted_max_readiness_ms": 1000.0},
    ],
)
def test_time_and_trusted_policy_fail_closed(overrides: dict[str, Any]) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    with pytest.raises(authority.AuthorityRecordInvalid):
        check(authority, example_records(), **overrides)


BINDING_CHANGES = [
    (kind, path, value)
    for kind in ("qualification", "readiness")
    for path, value in [
        (("manifest_digest",), "0" * 64),
        (("runtime_id",), "66666666-6666-4666-8666-666666666666"),
        (("runtime_generation",), "99"),
        (("qualification_id",), "77777777-7777-4777-8777-777777777777"),
        (("broker_identity", "run_id"), "0" * 40),
        (("broker_identity", "instance_id"), "88888888-8888-4888-8888-888888888888"),
        (("broker_identity", "endpoint", "host"), "other.example.test"),
        (("broker_identity", "endpoint", "port"), 6381),
        (("broker_identity", "tls_server_name"), "other.example.test"),
        (("broker_identity", "tls_spki_sha256"), "0" * 64),
        (("guard_identity", "revision"), "99"),
        (("guard_identity", "config_sha256"), "0" * 64),
        (("guard_identity", "control_id"), "99999999-9999-4999-8999-999999999999"),
    ]
] + [
    ("qualification", ("qualification_generation",), "99"),
    ("qualification", ("execution_config_digest",), "0" * 64),
    ("readiness", ("qualification_generation",), "99"),
    ("readiness", ("verifier_identity", "id"), "credential/example-other"),
    ("readiness", ("verifier_identity", "generation"), "99"),
]


@pytest.mark.parametrize("kind,path,value", BINDING_CHANGES)
def test_each_authority_binding_mismatch_refuses(
    kind: str, path: tuple[str, ...], value: Any
) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    records = example_records()
    at(records[kind], path[:-1])[path[-1]] = value
    with pytest.raises(authority.AuthorityRecordInvalid):
        check(authority, records)


def test_broker_restart_refuses_even_when_all_stored_records_agree() -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    records = example_records()
    actual = copy.deepcopy(records["manifest"]["broker_identity"])
    actual["run_id"] = "0" * 40
    with pytest.raises(authority.AuthorityRecordInvalid):
        check(authority, records, broker_identity=actual)


@pytest.mark.parametrize(
    "path,value",
    [
        (("instance_id",), "88888888-8888-4888-8888-888888888888"),
        (("endpoint", "host"), "other.example.test"),
        (("endpoint", "port"), 6381),
        (("tls_server_name",), "other.example.test"),
        (("tls_spki_sha256",), "0" * 64),
        (("run_id",), "0" * 40),
        (("database",), 1),
    ],
)
def test_every_actual_broker_identity_component_is_bound(path: tuple[str, ...], value: Any) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    records = example_records()
    actual = copy.deepcopy(records["manifest"]["broker_identity"])
    at(actual, path[:-1])[path[-1]] = value
    with pytest.raises(authority.AuthorityRecordInvalid):
        check(authority, records, broker_identity=actual)


@pytest.mark.parametrize("kind", ["manifest", "qualification", "readiness"])
def test_size_limit_applies_to_valid_padded_json(kind: str) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    raw = encoded(example_records()[kind])
    limit = raw + b" " * (16384 - len(raw))
    assert parse(authority, kind, limit).canonical_bytes == raw
    with pytest.raises(authority.AuthorityRecordInvalid):
        parse(authority, kind, limit + b" ")


MANIFEST_SCALARS = [
    path
    for kind, path in REQUIRED_FIELDS
    if kind == "manifest"
    and (not isinstance(at(example_records()[kind], path), dict))
    and (path[-1] not in {"schema_version", "database"})
]


@pytest.mark.parametrize("path", MANIFEST_SCALARS)
def test_each_tuple_component_changes_manifest_identity(path: tuple[str, ...]) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    data = example_records()["manifest"]
    original = parse(authority, "manifest", data)
    value = at(data, path)
    if isinstance(value, int):
        changed: Any = value + 1
    elif path[-1] == "kind":
        changed = "kubernetes"
    elif value.startswith("sha256:"):
        changed = "sha256:" + "0" * 64
    elif len(value) in (40, 64) and set(value) <= set("0123456789abcdef"):
        changed = "0" * len(value)
    elif "-" in value and len(value) == 36:
        changed = value[:-1] + ("0" if value[-1] != "0" else "1")
    elif value.isdecimal():
        changed = str(int(value) + 1)
    elif path[-1] in {"host", "tls_server_name"}:
        changed = "other.example.test"
    else:
        changed = value + "-next"
    at(data, path[:-1])[path[-1]] = changed
    updated = parse(authority, "manifest", data)
    assert updated.digest != original.digest
    assert updated.canonical_bytes != original.canonical_bytes


def test_wrong_record_kind_cannot_substitute_for_authority() -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    records = example_records()
    manifest = parse(authority, "manifest", records["manifest"])
    qualification = parse(authority, "qualification", records["qualification"])
    readiness = parse(authority, "readiness", records["readiness"])
    with pytest.raises(authority.AuthorityRecordInvalid):
        authority.validate_authority(
            manifest,
            readiness,
            qualification,
            broker_identity=records["manifest"]["broker_identity"],
            broker_now_ms=1500,
            trusted_max_readiness_ms=1000,
        )


def test_invalid_error_never_echoes_untrusted_input() -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    authority = authority_module()
    marker = "example-do-not-echo-marker"
    data = example_records()["manifest"]
    data[marker] = marker
    with pytest.raises(authority.AuthorityRecordInvalid) as error:
        parse(authority, "manifest", data)
    assert isinstance(error.value, ValueError)
    assert marker not in str(error.value)
