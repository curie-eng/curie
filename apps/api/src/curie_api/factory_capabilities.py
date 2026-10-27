"""Read the factory capability claim from an immutable, validated bundle."""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
from pathlib import Path
from typing import Literal

from plugin_format import bundle_root
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictStr, model_validator

from curie_api import bundles
from curie_api.config import Settings

CAPABILITIES_FILE = Path("verification/capabilities.json")
_HEX = r"^[0-9a-f]{64}$"
_IMAGE = r"^(?:[^@\s]+@)?sha256:[0-9a-f]{64}$"
_REGISTRY_IMAGE = r"^[^@\s]+@sha256:[0-9a-f]{64}$"


class CapabilityUnknown(ValueError):
    """The stored artifact cannot establish the claimed environment."""


class _Claim(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Executable(_Claim):
    name: StrictStr = Field(min_length=1)
    version: StrictStr = Field(min_length=1)
    path: StrictStr = Field(min_length=1)
    command: tuple[StrictStr, ...] = Field(min_length=1)


class RequiredExecutable(_Claim):
    name: StrictStr = Field(min_length=1)
    command: tuple[StrictStr, ...] = Field(min_length=1)


class CapabilityInputs(_Claim):
    required_executables: tuple[RequiredExecutable, ...]
    registry_hosts: tuple[StrictStr, ...]
    services: tuple[StrictStr, ...]
    egress_class: StrictStr = Field(min_length=1)


class Check(_Claim):
    id: StrictStr = Field(min_length=1)
    paths: tuple[StrictStr, ...] = Field(min_length=1)
    command: tuple[StrictStr, ...] = Field(min_length=1)
    install: tuple[StrictStr, ...] | None = None
    delegated_to: StrictStr | None = None


class Verification(_Claim):
    source: Literal["bundle", "repository"]
    checks: tuple[Check, ...]
    lockfile_installs: StrictBool

    @model_validator(mode="after")
    def check_source(self) -> Verification:
        if self.source != "bundle" and self.checks:
            raise ValueError("only bundle verification may claim checks at build time")
        if self.source == "bundle" and not self.checks:
            raise ValueError("bundle verification must claim at least one check")
        return self


class Publication(_Claim):
    github_directory_writable: Literal[False]
    code_host_credential_in_sandbox: Literal[False]


class CapabilityManifest(_Claim):
    schema_version: Literal[1]
    runner_image: StrictStr = Field(pattern=_IMAGE)
    bundle_input_sha256: StrictStr = Field(pattern=_HEX)
    executables: tuple[Executable, ...]
    registry_hosts: tuple[StrictStr, ...]
    services: tuple[StrictStr, ...]
    connectors: tuple[StrictStr, ...]
    verification: Verification
    egress_class: StrictStr = Field(min_length=1)
    publication: Publication

    @model_validator(mode="after")
    def check_unique_names(self) -> CapabilityManifest:
        for values in (
            tuple(item.name for item in self.executables),
            self.registry_hosts,
            self.services,
            self.connectors,
            tuple(check.id for check in self.verification.checks),
        ):
            if len(values) != len(set(values)):
                raise ValueError("capability lists must not contain duplicate names")
        return self


class CapabilityRead(_Claim):
    manifest: CapabilityManifest
    manifest_sha256: StrictStr = Field(pattern=_HEX)
    bundle_sha256: StrictStr = Field(pattern=_HEX)


def bundle_input_digest(root: Path) -> str:
    """Hash sorted file paths and bytes, excluding the manifest itself."""

    paths = sorted(
        (p for p in root.rglob("*") if p.relative_to(root) != CAPABILITIES_FILE),
        key=lambda p: p.relative_to(root).as_posix().encode("utf-8"),
    )
    digest = hashlib.sha256()
    for path in paths:
        if path.is_dir():
            continue
        if not path.is_file() or path.is_symlink():
            raise CapabilityUnknown("bundle contains a non-file capability input")
        name = path.relative_to(root).as_posix().encode("utf-8")
        content = path.read_bytes()
        digest.update(len(name).to_bytes(8, "big"))
        digest.update(name)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def read_manifest(
    data: bytes,
    settings: Settings,
    *,
    expected_bundle_sha256: str | None = None,
    required: bool = True,
    require_registry_image: bool = False,
) -> CapabilityRead | None:
    """Parse a bounded bundle and verify its claim against its source files."""

    archive_sha = hashlib.sha256(data).hexdigest()
    if expected_bundle_sha256 is not None and archive_sha != expected_bundle_sha256:
        raise CapabilityUnknown("stored bundle digest differs from its version")
    with tempfile.TemporaryDirectory() as tmp:
        dest = Path(tmp)
        try:
            bundles.extract_stored_bundle(
                data,
                dest,
                max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
                max_compression_ratio=settings.bundle_max_compression_ratio,
                max_members=settings.bundle_max_members,
            )
            root = bundle_root(dest)
            path = root / CAPABILITIES_FILE
            if not path.is_file():
                if required:
                    raise CapabilityUnknown("capability manifest is absent")
                return None
            raw = path.read_bytes()
            if len(raw) > 64 * 1024:
                raise CapabilityUnknown("capability manifest exceeds 64 KiB")
            manifest = CapabilityManifest.model_validate_json(raw)
            if require_registry_image and not re.fullmatch(
                _REGISTRY_IMAGE, manifest.runner_image
            ):
                raise CapabilityUnknown("capability manifest runner is not registry pinned")
            if manifest.bundle_input_sha256 != bundle_input_digest(root):
                raise CapabilityUnknown("capability manifest bundle input digest mismatch")
            inputs_path = root / "verification/capability-inputs.json"
            inputs = CapabilityInputs.model_validate_json(inputs_path.read_bytes())
            if (
                manifest.registry_hosts != inputs.registry_hosts
                or manifest.services != inputs.services
                or manifest.egress_class != inputs.egress_class
                or tuple((item.name, item.command) for item in manifest.executables)
                != tuple((item.name, item.command) for item in inputs.required_executables)
            ):
                raise CapabilityUnknown("capability manifest differs from declared inputs")
            lock = bundles.read_connector_lock(root)
            if lock is None or lock.runner is None or manifest.runner_image != lock.runner.image:
                raise CapabilityUnknown("capability manifest runner image differs from bundle lock")
            names = tuple(sorted(bundles.read_connectors(root).connectors))
            if tuple(sorted(manifest.connectors)) != names:
                raise CapabilityUnknown("capability manifest connectors differ from bundle")
            checks_path = root / "verification/checks.json"
            if checks_path.is_file():
                checks_raw = json.loads(checks_path.read_text(encoding="utf-8"))
                checks = checks_raw.get("checks", [])
                expected = tuple(Check.model_validate(check) for check in checks)
                if manifest.verification.lockfile_installs != checks_raw.get(
                    "lockfile_installs", False
                ):
                    raise CapabilityUnknown(
                        "capability manifest install policy differs from bundle"
                    )
                if checks and (
                    manifest.verification.source != "bundle"
                    or manifest.verification.checks != expected
                ):
                    raise CapabilityUnknown("capability manifest verification differs from bundle")
                if not checks and manifest.verification.source == "bundle":
                    raise CapabilityUnknown("capability manifest claims absent bundle checks")
            elif manifest.verification.source == "bundle":
                raise CapabilityUnknown("capability manifest claims absent bundle checks")
            return CapabilityRead(
                manifest=manifest,
                manifest_sha256=hashlib.sha256(raw).hexdigest(),
                bundle_sha256=archive_sha,
            )
        except CapabilityUnknown:
            raise
        except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
            raise CapabilityUnknown("capability manifest is invalid or unreadable") from exc
