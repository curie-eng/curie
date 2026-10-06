"""Which attached files are this message's, and which the sandbox lacks (ADR 0205).

A thread's earlier files are rebuilt on every boot (decision 3), so the
attachments mount holds more than the message that started this sandbox. The
worker says which file is which through an optional boot env key, the
attachment manifest; the chart's ``attachments-init`` writes what it actually
materialized to a hidden status file inside the mount. :func:`reconcile` folds
both against the files really on disk, and the disk is the final word
(decision 8): a file that is there is listed whatever either record says, and
a file that is not there is reported missing whatever either record says.

A manifest is data, never prompt text. Everything that leaves this module is a
bare on-disk file name that passed :func:`_safe_name`, an absolute path under
the mount, or a fixed phrase chosen by :func:`describe_reason`. URLs, channel
file ids and reason codes from the manifest never reach the model.

No manifest (a worker older than ADR 0205, or a malformed env value) reads as
"every file on disk arrived with this message", which is the behavior before
the manifest existed, and the status file is then ignored. With a manifest,
status entries for names it does not list are ignored too.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from aci_protocol import BootEnv

logger = logging.getLogger("curie_runner")

# The optional boot env key the worker sets, declared once on ``BootEnv``. It is
# outside the frozen ``SessionConfig``; absent means "no manifest", never "no
# files".
MANIFEST_ENV = BootEnv.env_key("attachments_manifest")

# Written by ``attachments-init`` inside the mount. Hidden, so the directory
# probe that discovers attachments never announces it as a file a person sent.
STATUS_FILE = ".curie-attachments-status.json"

_VERSION = 1

# Why a file could not be brought into the sandbox. Closed: any other value is
# rendered with one generic phrase, so a manifest cannot write text into the
# prompt through this field.
REASON_CODES: frozenset[str] = frozenset(
    {
        "no_route",
        "no_credential",
        "not_found",
        "forbidden",
        "rate_limited",
        "timeout",
        "digest_changed",
        "expired",
        "deadline",
        "fetch_failed",
    }
)

_REASON_PHRASES: dict[str, str] = {
    "no_route": "there is no configured way to fetch it from where it was sent",
    "no_credential": "the credential needed to fetch it was not available",
    "not_found": "it no longer exists where it was sent",
    "forbidden": "access to it was refused",
    "rate_limited": "the place it was sent from was limiting requests",
    "timeout": "fetching it took too long",
    "digest_changed": "it changed after it was sent, so this copy could not be verified",
    "expired": "the link to fetch it had lapsed",
    "deadline": "there was not enough time left to fetch it before this run began",
    "fetch_failed": "fetching it failed",
}
_GENERIC_PHRASE = "it could not be brought into this sandbox"

# Recorded when nothing says why a file is absent: rendered generically.
_UNRECORDED = "unrecorded"

_MAX_NAME_LENGTH = 255
# The init container writes one short entry per file; anything far larger is
# not its output.
_MAX_STATUS_BYTES = 1024 * 1024


def describe_reason(code: str) -> str:
    """A plain-words phrase for ``code``; never the code itself."""

    return _REASON_PHRASES.get(code, _GENERIC_PHRASE)


def _safe_name(value: object) -> str | None:
    """``value`` if it is a bare file name that can sit in the mount, else None.

    The mount is flat, so a name with a separator, a dot-prefix (hidden, never
    discovered) or a control character cannot be a file this sandbox holds.
    Rejecting rather than repairing keeps a crafted name from reaching a prompt.
    A backtick is rejected too, so a name rendered as quoted data cannot close
    its own quote.
    """

    if not isinstance(value, str) or not value or len(value) > _MAX_NAME_LENGTH:
        return None
    if value.startswith(".") or any(ch in value for ch in "/\\`"):
        return None
    if any(not ch.isprintable() for ch in value):
        return None
    if value != value.strip():
        return None
    return value


def _reason_code(value: object) -> str:
    return value if isinstance(value, str) and value in REASON_CODES else _UNRECORDED


@dataclass(frozen=True)
class ManifestFile:
    name: str
    current: bool


@dataclass(frozen=True)
class MissingAttachment:
    """A file the conversation holds that this sandbox does not."""

    name: str
    reason: str


@dataclass(frozen=True)
class Manifest:
    files: tuple[ManifestFile, ...]
    unavailable: tuple[MissingAttachment, ...]
    omitted: tuple[str, ...]
    ledger_unavailable: bool


@dataclass(frozen=True)
class FileStatus:
    """What ``attachments-init`` did with one reference."""

    name: str
    ok: bool
    reason: str


@dataclass(frozen=True)
class AttachmentView:
    """The reconciled picture the preamble and the first-prompt notice render."""

    on_disk: tuple[Path, ...]
    current: tuple[Path, ...]
    missing: tuple[MissingAttachment, ...]
    omitted: tuple[str, ...]
    ledger_unavailable: bool


def _dicts(value: object) -> list[dict[str, Any]] | None:
    if not isinstance(value, list):
        return None
    return [entry for entry in value if isinstance(entry, dict)]


def _versioned(raw: str) -> dict[str, Any] | None:
    try:
        data = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(data, dict):
        return None
    version = data.get("v")
    if isinstance(version, bool) or version != _VERSION:
        return None
    return data


def parse_manifest(raw: str | None) -> Manifest | None:
    """The worker's manifest, or None when it is absent, malformed or unknown.

    A wrongly typed container rejects the whole manifest. An individual entry
    with an unusable name is dropped, so one hostile name does not cost the
    turn its legitimate files.
    """

    if raw is None or not raw.strip():
        return None
    data = _versioned(raw)
    if data is None:
        logger.warning("attachment manifest is not a v%d object; ignoring it", _VERSION)
        return None
    files_raw = _dicts(data.get("files", []))
    unavailable_raw = _dicts(data.get("unavailable", []))
    omitted_raw = data.get("omitted", [])
    ledger = data.get("ledger_unavailable", False)
    if (
        files_raw is None
        or unavailable_raw is None
        or not isinstance(omitted_raw, list)
        or not isinstance(ledger, bool)
    ):
        logger.warning("attachment manifest has a malformed field; ignoring it")
        return None

    files: list[ManifestFile] = []
    for entry in files_raw:
        name = _safe_name(entry.get("name"))
        if name is not None:
            files.append(ManifestFile(name=name, current=entry.get("current") is True))
    unavailable: list[MissingAttachment] = []
    for entry in unavailable_raw:
        name = _safe_name(entry.get("name"))
        if name is not None:
            unavailable.append(MissingAttachment(name, _reason_code(entry.get("reason"))))
    omitted = [name for name in map(_safe_name, omitted_raw) if name is not None]
    return Manifest(
        files=tuple(files),
        unavailable=tuple(unavailable),
        omitted=tuple(omitted),
        ledger_unavailable=ledger,
    )


def read_status(mount: Path | None) -> dict[str, FileStatus] | None:
    """``attachments-init``'s per-file outcome by name, or None if unreadable."""

    if mount is None:
        return None
    try:
        with (mount / STATUS_FILE).open("rb") as handle:
            payload = handle.read(_MAX_STATUS_BYTES + 1)
    except OSError:
        return None
    if len(payload) > _MAX_STATUS_BYTES:
        logger.warning("attachment status file is over %d bytes; ignoring it", _MAX_STATUS_BYTES)
        return None
    try:
        raw = payload.decode("utf-8")
    except UnicodeDecodeError:
        return None
    data = _versioned(raw)
    entries = _dicts(data.get("files")) if data is not None else None
    if entries is None:
        return None
    status: dict[str, FileStatus] = {}
    for entry in entries:
        name = _safe_name(entry.get("name"))
        outcome = entry.get("status")
        if name is None or outcome not in ("ok", "unavailable"):
            continue
        status[name] = FileStatus(
            name=name, ok=outcome == "ok", reason=_reason_code(entry.get("reason"))
        )
    return status


def _dedupe(names: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(names))


def reconcile(
    manifest: Manifest | None,
    status: dict[str, FileStatus] | None,
    disk: Sequence[Path],
) -> AttachmentView:
    """Fold the manifest and the status file against the files on disk."""

    if manifest is None:
        paths = tuple(disk)
        return AttachmentView(
            on_disk=paths, current=paths, missing=(), omitted=(), ledger_unavailable=False
        )

    by_name = {path.name: path for path in disk}
    status = status or {}
    unavailable_reason = {entry.name: entry.reason for entry in manifest.unavailable}
    expected = _dedupe(
        [entry.name for entry in manifest.files] + [entry.name for entry in manifest.unavailable]
    )

    on_disk = [by_name[name] for name in expected if name in by_name]
    listed = {path.name for path in on_disk}
    # A file on disk that neither record names is still a file the agent can
    # open. It is listed after the manifested ones, so "current last" holds only
    # among files whose arrival order the manifest records.
    extras = [path for path in disk if path.name not in listed]
    on_disk.extend(extras)

    current = tuple(
        by_name[name]
        for name in _dedupe(entry.name for entry in manifest.files if entry.current)
        if name in by_name
    )

    missing: list[MissingAttachment] = []
    for name in expected:
        if name in by_name:
            continue
        outcome = status.get(name)
        if outcome is not None and not outcome.ok and outcome.reason != _UNRECORDED:
            reason = outcome.reason
        else:
            reason = unavailable_reason.get(name, _UNRECORDED)
        missing.append(MissingAttachment(name=name, reason=reason))
    if missing and extras:
        # Decision 4 makes the manifest name the on-disk name. A file reported
        # missing beside an unmanifested one on disk usually means the two
        # disagree (normalization, a renaming substrate), not a lost file.
        logger.warning(
            "attachment manifest names %d file(s) absent from disk while %d "
            "unmanifested file(s) are present; the names may disagree",
            len(missing),
            len(extras),
        )

    accounted = set(expected) | set(by_name)
    omitted = tuple(name for name in _dedupe(manifest.omitted) if name not in accounted)
    return AttachmentView(
        on_disk=tuple(on_disk),
        current=current,
        missing=tuple(missing),
        omitted=omitted,
        ledger_unavailable=manifest.ledger_unavailable,
    )
