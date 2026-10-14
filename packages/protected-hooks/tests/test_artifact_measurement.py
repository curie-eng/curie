"""Structural measurement and actual filesystem refusals, @spec PROTECTED-HOOK-LANE-2."""

from __future__ import annotations

import copy
import hashlib
import importlib
import importlib.util
import json
import os
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.authority_records import parse_manifest, parse_qualification


def codec() -> Any:
    """Make missing implementation an explicit RED assertion."""
    assert importlib.util.find_spec("curie_protected_hooks.artifact_measurement") is not None, (
        "artifact measurement codec and bound directory loader are not implemented"
    )
    return importlib.import_module("curie_protected_hooks.artifact_measurement")


def encoded(value: Any) -> bytes:
    """Independent encoding oracle named by the source specification."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


def examples() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Independently authored fictional measurements, never runtime qualification."""
    manifest = {
        "schema_version": 1,
        "runtime_id": "11111111-1111-4111-8111-111111111111",
        "runtime_generation": "1",
        "broker_identity": {
            "instance_id": "22222222-2222-4222-8222-222222222222",
            "endpoint": {"host": "broker.example.com", "port": 6380},
            "tls_server_name": "broker.example.com",
            "tls_spki_sha256": "a" * 64,
            "run_id": "b" * 40,
            "database": 0,
        },
        "worker_image_digest": "sha256:" + "c" * 64,
        "runner_image_digest": "sha256:" + "d" * 64,
        "bundle_digest": {"sha256": "e" * 64, "object_identity": "bundle/acme-v1"},
        "execution_config_digest": "f" * 64,
        "qualification_id": "33333333-3333-4333-8333-333333333333",
        "substrate": {
            "kind": "docker",
            "authority_domain_id": "44444444-4444-4444-8444-444444444444",
            "launch_identity": "launch/acme-v1",
            "launch_config_sha256": "0" * 64,
        },
        "guard_identity": {
            "control_id": "55555555-5555-4555-8555-555555555555",
            "revision": "1",
            "config_sha256": "1" * 64,
        },
        "credential_refs": {
            "enqueue": {"id": "credential/acme-enqueue", "generation": "1"},
            "worker": {"id": "credential/acme-worker", "generation": "2"},
            "verifier": {"id": "credential/acme-verifier", "generation": "3"},
        },
    }
    measurement = {
        "schema_version": 1,
        "kind": "protected-artifact-qualification",
        "manifest_digest": hashlib.sha256(encoded(manifest)).hexdigest(),
        "execution_config_digest": "f" * 64,
        "substrate": copy.deepcopy(manifest["substrate"]),
        "guard_identity": copy.deepcopy(manifest["guard_identity"]),
        "platform_mappings": [
            {
                "os": "linux",
                "architecture": "amd64",
                "variant": None,
                "runtime_profile": "profile/acme-v1",
                "pinned_image_reference": "registry.example.com/acme/runner@sha256:" + "d" * 64,
                "oci_digest": "sha256:" + "d" * 64,
                "observed_image_id": "docker-pullable://registry.example.com/acme/runner@sha256:"
                + "d" * 64,
                "image_config_id": "sha256:" + "2" * 64,
            }
        ],
        "launch_projection": {
            "schema_version": 1,
            "format": "profile/unsupported-v1",
            "sha256": "0" * 64,
        },
        "delivery_slots": {
            "schema_version": 1,
            "execution_identity": "verified-delivery-v1",
            "history_binding": "protected-history-v1",
            "runner_token": "fresh-owned-launch-v1",
            "history_token": "scoped-history-credential-v1",
            "workload_identity": "control-plane-cold-create-v1",
            "dial_identity": "owned-workload-endpoint-v1",
        },
        "enforcement_evidence_ref": "sha256:" + "3" * 64,
    }
    qualification = {
        "schema_version": 1,
        "qualification_id": manifest["qualification_id"],
        "qualification_generation": "1",
        "runtime_id": manifest["runtime_id"],
        "runtime_generation": manifest["runtime_generation"],
        "manifest_digest": measurement["manifest_digest"],
        "broker_identity": copy.deepcopy(manifest["broker_identity"]),
        "execution_config_digest": manifest["execution_config_digest"],
        "guard_identity": copy.deepcopy(manifest["guard_identity"]),
        "measurement_record_id": "sha256:" + hashlib.sha256(encoded(measurement)).hexdigest(),
    }
    return manifest, qualification, measurement


def test_canonical_record_is_immutable_and_has_fresh_copies() -> None:
    """Retaining mutable input or decoded state must not preserve a stale digest."""
    mod = codec()
    _, _, data = examples()
    raw = encoded(data)
    record = mod.parse_artifact_measurement(raw)
    assert record.canonical_bytes == raw
    assert record.digest == hashlib.sha256(raw).hexdigest()
    decoded = record.as_dict()
    decoded["platform_mappings"][0]["observed_image_id"] = "different"
    assert record.as_dict() == data
    data["kind"] = "different"
    assert record.canonical_bytes == raw
    with pytest.raises((FrozenInstanceError, AttributeError)):
        record._canonical_bytes = b"{}"
    with pytest.raises(mod.ArtifactMeasurementInvalid):
        mod.ArtifactMeasurement(bytearray(raw))


@pytest.mark.parametrize("raw", [b"{}", b"\xff", b"null", b"[]", b"NaN", b"Infinity", b"1.0"])
def test_malformed_records_refuse_without_original_exception(raw: bytes) -> None:
    mod = codec()
    with pytest.raises(mod.ArtifactMeasurementInvalid) as error:
        mod.parse_artifact_measurement(raw)
    assert error.value.__suppress_context__
    assert error.value.__cause__ is None


@pytest.mark.parametrize(
    "form",
    ["whitespace", "order", "duplicate", "nested_duplicate", "float", "trailing", "oversize"],
)
def test_noncanonical_or_unsafe_bytes_refuse(form: str) -> None:
    mod = codec()
    _, _, data = examples()
    raw = encoded(data)
    if form == "whitespace":
        raw += b"\n"
    elif form == "order":
        raw = json.dumps(data, separators=(",", ":")).encode()
    elif form == "duplicate":
        raw = raw.replace(b'"schema_version":1', b'"schema_version":1,"schema_version":1', 1)
    elif form == "nested_duplicate":
        raw = raw.replace(
            b'"architecture":"amd64"', b'"architecture":"amd64","architecture":"amd64"'
        )
    elif form == "float":
        raw = raw.replace(b'"schema_version":1', b'"schema_version":1.0', 1)
    elif form == "trailing":
        raw += b"{}"
    else:
        raw = b" " * 262145
    with pytest.raises(mod.ArtifactMeasurementInvalid):
        mod.parse_artifact_measurement(raw)


OBJECT_PATHS = [
    (),
    ("substrate",),
    ("guard_identity",),
    ("launch_projection",),
    ("delivery_slots",),
    ("platform_mappings", 0),
]


def at(data: dict[str, Any], path: tuple[Any, ...]) -> Any:
    value: Any = data
    for part in path:
        value = value[part]
    return value


@pytest.mark.parametrize("path", OBJECT_PATHS)
def test_unknown_and_missing_fields_refuse_at_every_object(path: tuple[Any, ...]) -> None:
    mod = codec()
    for violation in ("unknown", "missing"):
        _, _, data = examples()
        target = at(data, path)
        if violation == "unknown":
            target["unknown"] = "secret-placeholder"
        else:
            del target[next(iter(target))]
        with pytest.raises(mod.ArtifactMeasurementInvalid):
            mod.parse_artifact_measurement(encoded(data))


@pytest.mark.parametrize("path", OBJECT_PATHS)
def test_null_refuses_except_the_two_mapping_slots(path: tuple[Any, ...]) -> None:
    mod = codec()
    _, _, original = examples()
    for field in at(original, path):
        if path == ("platform_mappings", 0) and field in {"variant", "image_config_id"}:
            continue
        data = copy.deepcopy(original)
        at(data, path)[field] = None
        with pytest.raises(mod.ArtifactMeasurementInvalid):
            mod.parse_artifact_measurement(encoded(data))
    original["platform_mappings"][0]["image_config_id"] = None
    assert mod.parse_artifact_measurement(encoded(original)).as_dict() == original


@pytest.mark.parametrize(
    "path,value",
    [
        (("schema_version",), True),
        (("schema_version",), 2),
        (("kind",), "other"),
        (("manifest_digest",), "A" * 64),
        (("execution_config_digest",), "0" * 63),
        (("enforcement_evidence_ref",), "https://example.com/file"),
        (("substrate", "kind"), "other"),
        (("guard_identity", "revision"), 1),
        (("launch_projection", "format"), " bad"),
        (("launch_projection", "sha256"), "1" * 64),
        (("delivery_slots", "runner_token"), "arbitrary-validator"),
        (("platform_mappings", 0, "os"), "Linux"),
        (("platform_mappings", 0, "architecture"), 64),
        (("platform_mappings", 0, "variant"), ""),
        (("platform_mappings", 0, "runtime_profile"), "../bad?"),
        (("platform_mappings", 0, "oci_digest"), "sha256:" + "D" * 64),
        (("platform_mappings", 0, "pinned_image_reference"), "@sha256:" + "d" * 64),
        (("platform_mappings", 0, "pinned_image_reference"), "acme:latest"),
        (("platform_mappings", 0, "pinned_image_reference"), "acme@sha256:" + "c" * 64),
        (("platform_mappings", 0, "observed_image_id"), "has whitespace"),
        (("platform_mappings", 0, "observed_image_id"), "é"),
        (("platform_mappings", 0, "image_config_id"), "a" * 2049),
    ],
)
def test_strict_scalar_and_cross_digest_refusals(path: tuple[Any, ...], value: Any) -> None:
    mod = codec()
    _, _, data = examples()
    at(data, path[:-1])[path[-1]] = value
    with pytest.raises(mod.ArtifactMeasurementInvalid):
        mod.parse_artifact_measurement(encoded(data))


def test_platform_mapping_cardinality_uniqueness_and_sorted_order() -> None:
    mod = codec()
    _, _, data = examples()
    base = data["platform_mappings"][0]
    for rows in ([], [copy.deepcopy(base)] * 2, [copy.deepcopy(base)] * 33):
        data["platform_mappings"] = rows
        with pytest.raises(mod.ArtifactMeasurementInvalid):
            mod.parse_artifact_measurement(encoded(data))
    rows = [{**base, "runtime_profile": f"profile/{n:02}"} for n in range(32)]
    data["platform_mappings"] = rows
    assert len(mod.parse_artifact_measurement(encoded(data)).as_dict()["platform_mappings"]) == 32
    data["platform_mappings"] = list(reversed(rows))
    with pytest.raises(mod.ArtifactMeasurementInvalid):
        mod.parse_artifact_measurement(encoded(data))


def test_exact_platform_observations_never_strip_scheme_or_infer_missing_identity() -> None:
    mod = codec()
    _, _, data = examples()
    record = mod.parse_artifact_measurement(encoded(data))
    row = data["platform_mappings"][0]
    observed = {
        key: row[key]
        for key in (
            "os",
            "architecture",
            "variant",
            "runtime_profile",
            "observed_image_id",
            "image_config_id",
        )
    }
    assert record.match_platform(**observed) == row
    for key, value in [
        ("os", "windows"),
        ("architecture", None),
        ("variant", "v8"),
        ("runtime_profile", "profile/other"),
        ("observed_image_id", row["oci_digest"]),
        ("image_config_id", None),
    ]:
        with pytest.raises(mod.ArtifactMeasurementInvalid):
            record.match_platform(**{**observed, key: value})


def test_selection_checks_every_manifest_and_authorization_binding() -> None:
    mod = codec()
    m, q, data = examples()
    record = mod.parse_artifact_measurement(encoded(data))
    manifest = parse_manifest(encoded(m))
    qualification = parse_qualification(encoded(q))
    mod.validate_artifact_selection(record, manifest, qualification)
    for field, value in [
        ("manifest_digest", "0" * 64),
        ("runtime_id", m["qualification_id"]),
        ("runtime_generation", "2"),
        ("qualification_id", m["runtime_id"]),
        ("execution_config_digest", "0" * 64),
        ("measurement_record_id", "sha256:" + "0" * 64),
        ("measurement_record_id", "measurement/legacy"),
    ]:
        changed = {**q, field: value}
        with pytest.raises(mod.ArtifactMeasurementInvalid):
            mod.validate_artifact_selection(record, manifest, parse_qualification(encoded(changed)))
    for field in ("broker_identity", "guard_identity"):
        changed = copy.deepcopy(q)
        changed[field]["run_id" if field == "broker_identity" else "config_sha256"] = "0" * (
            40 if field == "broker_identity" else 64
        )
        with pytest.raises(mod.ArtifactMeasurementInvalid):
            mod.validate_artifact_selection(record, manifest, parse_qualification(encoded(changed)))
    for field in (
        "execution_config_digest",
        "guard_identity",
        "substrate",
        "platform_mappings",
        "manifest_digest",
    ):
        changed = copy.deepcopy(data)
        if field in ("manifest_digest", "execution_config_digest"):
            changed[field] = "0" * 64
        elif field == "guard_identity":
            changed[field]["config_sha256"] = "0" * 64
        elif field == "substrate":
            changed[field]["launch_identity"] = "launch/other"
        else:
            changed[field][0]["oci_digest"] = "sha256:" + "c" * 64
            changed[field][0]["pinned_image_reference"] = "acme@sha256:" + "c" * 64
        changed_q = {
            **q,
            "measurement_record_id": "sha256:" + hashlib.sha256(encoded(changed)).hexdigest(),
        }
        with pytest.raises(mod.ArtifactMeasurementInvalid):
            mod.validate_artifact_selection(
                mod.parse_artifact_measurement(encoded(changed)),
                manifest,
                parse_qualification(encoded(changed_q)),
            )
    for bad_m, bad_q in [(m, qualification), (manifest, q), (None, qualification)]:
        with pytest.raises(mod.ArtifactMeasurementInvalid):
            mod.validate_artifact_selection(record, bad_m, bad_q)


def install(root: Path) -> tuple[Any, Any, Path, bytes]:
    m, q, data = examples()
    raw = encoded(data)
    path = root / (q["measurement_record_id"][7:] + ".json")
    path.write_bytes(raw)
    return parse_manifest(encoded(m)), parse_qualification(encoded(q)), path, raw


def test_bound_directory_loads_only_qualification_selected_content(tmp_path: Path) -> None:
    mod = codec()
    manifest, qualification, path, raw = install(tmp_path)
    with mod.ArtifactMeasurementDirectory(str(tmp_path)) as directory:
        result = directory.load(manifest=manifest, qualification=qualification)
        assert result.canonical_bytes == raw
        path.write_bytes(raw + b" ")
        with pytest.raises(mod.ArtifactMeasurementInvalid):
            directory.load(manifest=manifest, qualification=qualification)
    with pytest.raises(mod.ArtifactMeasurementInvalid):
        directory.load(manifest=manifest, qualification=qualification)


@pytest.mark.parametrize(
    "kind", ["missing", "symlink", "directory", "fifo", "oversize", "different", "malformed"]
)
def test_file_loader_refuses_nonregular_or_changed_content(tmp_path: Path, kind: str) -> None:
    mod = codec()
    manifest, qualification, path, raw = install(tmp_path)
    path.unlink()
    if kind == "symlink":
        target = tmp_path / "target.json"
        target.write_bytes(raw)
        path.symlink_to(target)
    elif kind == "directory":
        path.mkdir()
    elif kind == "fifo":
        os.mkfifo(path)
    elif kind == "oversize":
        path.write_bytes(b"x" * 262145)
    elif kind == "different":
        _, _, data = examples()
        data["enforcement_evidence_ref"] = "sha256:" + "4" * 64
        path.write_bytes(encoded(data))
    elif kind == "malformed":
        path.write_bytes(b"secret-placeholder")
    with mod.ArtifactMeasurementDirectory(str(tmp_path)) as directory:
        with pytest.raises(mod.ArtifactMeasurementInvalid) as error:
            directory.load(manifest=manifest, qualification=qualification)
        assert error.value.__suppress_context__
        assert error.value.__cause__ is None
        assert "secret-placeholder" not in str(error.value)


def test_root_refuses_relative_traversal_uri_and_all_symlink_components(tmp_path: Path) -> None:
    mod = codec()
    root = tmp_path / "real"
    root.mkdir()
    inner = root / "inner"
    inner.mkdir()
    link = tmp_path / "linked"
    link.symlink_to(root, target_is_directory=True)
    for value in (
        "relative",
        "https://example.com/dir",
        str(root) + "/../real",
        str(link),
        str(link / "inner"),
    ):
        with pytest.raises(mod.ArtifactMeasurementInvalid):
            mod.ArtifactMeasurementDirectory(value)


def test_directory_descriptor_stays_bound_after_path_replacement(tmp_path: Path) -> None:
    mod = codec()
    root = tmp_path / "root"
    root.mkdir()
    manifest, qualification, _, raw = install(root)
    with mod.ArtifactMeasurementDirectory(str(root)) as directory:
        root.rename(tmp_path / "original")
        root.mkdir()
        _, _, path, _ = install(root)
        path.write_bytes(b"different")
        assert directory.load(manifest=manifest, qualification=qualification).canonical_bytes == raw


def test_invalid_selection_never_uses_a_caller_filename(tmp_path: Path) -> None:
    mod = codec()
    manifest, qualification, _, _ = install(tmp_path)
    with mod.ArtifactMeasurementDirectory(str(tmp_path)) as directory:
        for bad in (qualification.as_dict(), "../secret-placeholder", None):
            with pytest.raises(mod.ArtifactMeasurementInvalid):
                directory.load(manifest=manifest, qualification=bad)


@pytest.mark.parametrize("entry", ["parser", "constructor"])
def test_malformed_json_does_not_retain_input_in_exception_graph(entry: str) -> None:
    """A suppressed JSONDecodeError still retains its original input document."""
    mod = codec()
    decode = mod.parse_artifact_measurement if entry == "parser" else mod.ArtifactMeasurement
    with pytest.raises(mod.ArtifactMeasurementInvalid) as error:
        decode(b'{"credential":"synthetic-secret",')
    assert error.value.__cause__ is None
    assert error.value.__context__ is None


@pytest.mark.parametrize("entry", ["root", "file"])
def test_actual_missing_paths_do_not_survive_in_exception_graph(tmp_path: Path, entry: str) -> None:
    """A retained OSError exposes path data even with a fixed outer message."""
    mod = codec()
    if entry == "root":
        with pytest.raises(mod.ArtifactMeasurementInvalid) as error:
            mod.ArtifactMeasurementDirectory(str(tmp_path / "synthetic-missing-root"))
    else:
        manifest, qualification, path, _ = install(tmp_path)
        path.unlink()
        with mod.ArtifactMeasurementDirectory(str(tmp_path)) as directory:
            with pytest.raises(mod.ArtifactMeasurementInvalid) as error:
                directory.load(manifest=manifest, qualification=qualification)
    assert error.value.__cause__ is None
    assert error.value.__context__ is None


@pytest.mark.parametrize(
    "entry", ["parser", "constructor", "selection", "platform", "root", "file", "closed"]
)
def test_failure_boundaries_detach_already_active_caller_exception(
    tmp_path: Path, entry: str
) -> None:
    """Python attaches ambient caller data at raise time, after inner handlers."""
    mod = codec()
    manifest, qualification, _, raw = install(tmp_path)
    record = mod.parse_artifact_measurement(raw)
    with mod.ArtifactMeasurementDirectory(str(tmp_path)) as directory:
        closed = mod.ArtifactMeasurementDirectory(str(tmp_path))
        closed.close()
        try:
            raise RuntimeError("synthetic-caller-credential")
        except RuntimeError:
            with pytest.raises(mod.ArtifactMeasurementInvalid) as error:
                if entry == "parser":
                    mod.parse_artifact_measurement(b"{}")
                elif entry == "constructor":
                    mod.ArtifactMeasurement(b"{}")
                elif entry == "selection":
                    mod.validate_artifact_selection(record, manifest, None)
                elif entry == "platform":
                    record.match_platform(
                        os="unsupported",
                        architecture="amd64",
                        variant=None,
                        runtime_profile="profile/acme-v1",
                        observed_image_id="synthetic-unsupported-image",
                        image_config_id=None,
                    )
                elif entry == "root":
                    mod.ArtifactMeasurementDirectory("synthetic-relative-root")
                elif entry == "file":
                    directory.load(manifest=manifest, qualification=None)
                else:
                    closed.__enter__()
            assert error.value.__cause__ is None
            assert error.value.__context__ is None
