"""Structural artifact measurements, @spec PROTECTED-HOOK-LANE-2.

Parsing and local file loading grant no qualification or launch authority. The
caller must independently establish provisioner ownership of the actual mount
and obtain current selection through authenticated qualification authorization.
No projection format or controlled launcher is qualified by this module.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass
from typing import Any, NoReturn

from curie_protected_hooks.authority_records import (
    _GUARD,
    _MANIFEST,
    Manifest,
    Qualification,
    _object,
    _pairs,
    _pattern,
    _reject_number,
    _require,
    _scalar,
)

_MAX_BYTES = 262144
_TOP_FIELDS = {
    "schema_version",
    "kind",
    "manifest_digest",
    "execution_config_digest",
    "substrate",
    "guard_identity",
    "platform_mappings",
    "launch_projection",
    "delivery_slots",
    "enforcement_evidence_ref",
}
_MAPPING_FIELDS = {
    "os",
    "architecture",
    "variant",
    "runtime_profile",
    "pinned_image_reference",
    "oci_digest",
    "observed_image_id",
    "image_config_id",
}
_DELIVERY_SLOTS = {
    "schema_version": 1,
    "execution_identity": "verified-delivery-v1",
    "history_binding": "protected-history-v1",
    "runner_token": "fresh-owned-launch-v1",
    "history_token": "scoped-history-credential-v1",
    "workload_identity": "control-plane-cold-create-v1",
    "dial_identity": "owned-workload-endpoint-v1",
}


class ArtifactMeasurementInvalid(ValueError):
    """Fixed diagnostic without file paths or original input, @spec PROTECTED-HOOK-LANE-2."""

    def __init__(self) -> None:
        """@spec PROTECTED-HOOK-LANE-2."""
        super().__init__("Invalid protected artifact measurement")


def _invalid() -> NoReturn:
    """Detach the actual propagating error, including ambient caller context."""
    error = ArtifactMeasurementInvalid()
    try:
        raise error from None
    finally:
        error.__context__ = None
        error.__cause__ = None


def _close_fd(fd: int) -> None:
    """Release an owned descriptor without retaining raw operating-system errors."""
    try:
        os.close(fd)
    except OSError:
        _invalid()


def _image_identity(value: Any) -> None:
    """Printable ASCII without whitespace, @spec PROTECTED-HOOK-LANE-2."""
    _pattern(value, r"[\x21-\x7e]{1,2048}")


def _platform_identity(row: dict[str, Any]) -> tuple[str, str, str, str]:
    """Null variant has the specified empty-string sort position."""
    return row["os"], row["architecture"], row["variant"] or "", row["runtime_profile"]


def _mapping(row: Any) -> None:
    """Validate exactly one closed platform mapping, @spec PROTECTED-HOOK-LANE-2."""
    _require(type(row) is dict and row.keys() == _MAPPING_FIELDS)
    for field in ("os", "architecture"):
        _pattern(row[field], r"[a-z0-9][a-z0-9._-]{0,63}")
    if row["variant"] is not None:
        _pattern(row["variant"], r"[a-z0-9][a-z0-9._-]{0,63}")
    _scalar(row["runtime_profile"], "opaque_ref")
    _scalar(row["oci_digest"], "oci_digest")
    for field in ("pinned_image_reference", "observed_image_id"):
        _image_identity(row[field])
    if row["image_config_id"] is not None:
        _image_identity(row["image_config_id"])
    repository, separator, digest = row["pinned_image_reference"].rpartition("@")
    _require(bool(repository) and separator == "@" and digest == row["oci_digest"])


def _canonical(raw: bytes, *, schema_version: int = 1) -> bytes:
    """Refuse stored bytes that differ from closed canonical encoding."""
    try:
        _require(type(raw) is bytes and 0 < len(raw) <= _MAX_BYTES)
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_pairs,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
        _require(type(value) is dict and value.keys() == _TOP_FIELDS)
        _require(schema_version in (1, 2))
        _require(type(value["schema_version"]) is int and value["schema_version"] == schema_version)
        _require(type(value["kind"]) is str and value["kind"] == "protected-artifact-qualification")
        for field in ("manifest_digest", "execution_config_digest"):
            _scalar(value[field], "sha256")
        _object(value["substrate"], _MANIFEST["substrate"])
        _object(value["guard_identity"], _GUARD)
        projection = value["launch_projection"]
        _require(
            type(projection) is dict
            and projection.keys() == {"schema_version", "format", "sha256"}
        )
        _require(
            type(projection["schema_version"]) is int
            and projection["schema_version"] == schema_version
        )
        _scalar(projection["format"], "opaque_ref")
        _scalar(projection["sha256"], "sha256")
        _require(value["launch_projection"]["sha256"] == value["substrate"]["launch_config_sha256"])
        slots = value["delivery_slots"]
        expected_slots = dict(_DELIVERY_SLOTS)
        if schema_version == 2:
            expected_slots.update(
                schema_version=2,
                lifecycle_context="guarded-lifecycle-context-v1",
                sdk_lifecycle="owned-cold-sdk-capsule-v1",
            )
            _require(value["substrate"]["kind"] == "kubernetes")
            _require(value["launch_projection"]["format"] == "kubernetes-custodied-pod-v1")
        _require(type(slots) is dict and slots.keys() == expected_slots.keys())
        _require(type(slots["schema_version"]) is int and slots["schema_version"] == schema_version)
        for field, expected in expected_slots.items():
            if field != "schema_version":
                _require(type(slots[field]) is str and slots[field] == expected)
        _scalar(value["enforcement_evidence_ref"], "oci_digest")
        rows = value["platform_mappings"]
        _require(type(rows) is list and 1 <= len(rows) <= 32)
        for row in rows:
            _mapping(row)
            if schema_version == 2:
                _require(row["os"] == "linux")
                _require(row["runtime_profile"] == "protected-sdk-custody-v1")
        identities = [_platform_identity(row) for row in rows]
        _require(len(set(identities)) == len(rows) and identities == sorted(identities))
        canonical = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        _require(canonical == raw)
        return canonical
    except (ValueError, TypeError, OverflowError, RecursionError):
        _invalid()


@dataclass(frozen=True, slots=True)
class ArtifactMeasurement:
    """Immutable structural record; no permission or qualification assertion."""

    _canonical_bytes: bytes

    def __post_init__(self) -> None:
        """Validate direct construction as well as parser calls."""
        object.__setattr__(self, "_canonical_bytes", _canonical(self._canonical_bytes))

    @property
    def canonical_bytes(self) -> bytes:
        """@spec PROTECTED-HOOK-LANE-2."""
        return self._canonical_bytes

    @property
    def digest(self) -> str:
        """Content identity, not provisioner authentication."""
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        """Fresh decoded copy, never retained mutable state."""
        result: dict[str, Any] = json.loads(self._canonical_bytes)
        return result

    def match_platform(
        self,
        *,
        os: str,
        architecture: str,
        variant: str | None,
        runtime_profile: str,
        observed_image_id: str,
        image_config_id: str | None,
    ) -> dict[str, Any]:
        """Match supplied exact observations; do not observe or authenticate a runtime."""
        try:
            supplied = {
                "os": os,
                "architecture": architecture,
                "variant": variant,
                "runtime_profile": runtime_profile,
                "observed_image_id": observed_image_id,
                "image_config_id": image_config_id,
            }
            for row in self.as_dict()["platform_mappings"]:
                if all(
                    type(supplied[key]) is type(row[key]) and supplied[key] == row[key]
                    for key in supplied
                ):
                    return dict(row)
            _invalid()
        except (ValueError, TypeError, OverflowError, RecursionError):
            _invalid()


def parse_artifact_measurement(raw: bytes) -> ArtifactMeasurement:
    """Parse structural canonical bytes only, @spec PROTECTED-HOOK-LANE-2."""
    return ArtifactMeasurement(raw)


@dataclass(frozen=True, slots=True, repr=False)
class ArtifactMeasurementV2:
    """Closed ADR 0245 structural claim, unsupported by v1 launch consumers.

    Parsing supplies neither runtime observations nor qualification. Full v2
    projection, custody and producer composition must precede consumer use.
    """

    _canonical_bytes: bytes

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "_canonical_bytes", _canonical(self._canonical_bytes, schema_version=2)
        )

    def __repr__(self) -> str:
        return "<ArtifactMeasurementV2>"

    @property
    def canonical_bytes(self) -> bytes:
        return self._canonical_bytes

    @property
    def digest(self) -> str:
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = json.loads(self._canonical_bytes)
        return value


def parse_artifact_measurement_v2(raw: bytes) -> ArtifactMeasurementV2:
    """Explicit schema2 parser; never upgrades or coerces a v1 record."""
    return ArtifactMeasurementV2(raw)


def _selection(manifest: Manifest, qualification: Qualification) -> str:
    """Qualification is the only selector; no independent filename is accepted."""
    _require(type(manifest) is Manifest and type(qualification) is Qualification)
    m, q = manifest.as_dict(), qualification.as_dict()
    _require(q["manifest_digest"] == manifest.digest)
    for field in (
        "runtime_id",
        "runtime_generation",
        "qualification_id",
        "broker_identity",
        "execution_config_digest",
        "guard_identity",
    ):
        _require(q[field] == m[field])
    _scalar(q["measurement_record_id"], "oci_digest")
    digest: str = q["measurement_record_id"][7:]
    return digest


def validate_artifact_selection(
    record: ArtifactMeasurement,
    manifest: Manifest,
    qualification: Qualification,
) -> None:
    """Match immutable trusted inputs, not a live authorization/readiness check."""
    try:
        digest = _selection(manifest, qualification)
        _require(type(record) is ArtifactMeasurement and record.digest == digest)
        value, m = record.as_dict(), manifest.as_dict()
        _require(value["manifest_digest"] == manifest.digest)
        for field in ("execution_config_digest", "substrate", "guard_identity"):
            _require(value[field] == m[field])
        for row in value["platform_mappings"]:
            _require(row["oci_digest"] == m["runner_image_digest"])
    except (ValueError, TypeError, OverflowError, RecursionError):
        _invalid()


def _open_root(root: str) -> int:
    """Walk every absolute path component without following symlinks."""
    fd = -1
    try:
        _require(type(root) is str and root.startswith("/"))
        components = root.split("/")[1:] if root != "/" else []
        _require(all(part not in ("", ".", "..") for part in components))
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        fd = os.open("/", flags)
        for part in components:
            child = os.open(part, flags, dir_fd=fd)
            _close_fd(fd)
            fd = child
        return fd
    except (OSError, ValueError, TypeError, OverflowError, RecursionError):
        if fd >= 0:
            _close_fd(fd)
        _invalid()


def _fingerprint(info: os.stat_result) -> tuple[int, int, int, int, int]:
    """Detect replacement or mutation visible during a bounded read."""
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


class ArtifactMeasurementDirectory:
    """Read-only structural file loader bound to an explicit directory descriptor.

    Filesystem metadata cannot authenticate mount ownership. Production callers
    need independent LANE-8 provisioning proof and authenticated current selection.
    This object supplies neither, and exposes no URI, callback or environment path.
    """

    __slots__ = ("_fd",)

    def __init__(self, root: str) -> None:
        """Bind the actual root without following any symlink component."""
        self._fd = _open_root(root)

    def __repr__(self) -> str:
        """Do not disclose configured paths in representations."""
        return "ArtifactMeasurementDirectory()"

    def __enter__(self) -> ArtifactMeasurementDirectory:
        """@spec PROTECTED-HOOK-LANE-2."""
        if self._fd < 0:
            _invalid()
        return self

    def __exit__(self, *_args: object) -> None:
        """Close the bound descriptor, including on a failed load."""
        self.close()

    def close(self) -> None:
        """Release only this loader's directory descriptor."""
        if self._fd >= 0:
            fd, self._fd = self._fd, -1
            _close_fd(fd)

    def load(self, *, manifest: Manifest, qualification: Qualification) -> ArtifactMeasurement:
        """Load only selected bounded canonical bytes; return structural evidence."""
        fd = -1
        try:
            digest = _selection(manifest, qualification)
            _require(self._fd >= 0)
            filename = digest + ".json"
            fd = os.open(
                filename,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                dir_fd=self._fd,
            )
            before = os.fstat(fd)
            _require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= _MAX_BYTES)
            chunks: list[bytes] = []
            size = 0
            while size <= _MAX_BYTES:
                chunk = os.read(fd, min(65536, _MAX_BYTES + 1 - size))
                if not chunk:
                    break
                chunks.append(chunk)
                size += len(chunk)
            _require(size <= _MAX_BYTES)
            after = os.fstat(fd)
            current = os.stat(filename, dir_fd=self._fd, follow_symlinks=False)
            _require(stat.S_ISREG(current.st_mode))
            _require(_fingerprint(before) == _fingerprint(after) == _fingerprint(current))
            raw = b"".join(chunks)
            _require(hashlib.sha256(raw).hexdigest() == digest)
            record = parse_artifact_measurement(raw)
            validate_artifact_selection(record, manifest, qualification)
            return record
        except (OSError, ValueError, TypeError, OverflowError, RecursionError):
            _invalid()
        finally:
            if fd >= 0:
                _close_fd(fd)
