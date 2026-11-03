"""Optional UI screenshot evidence from the managed workspace.

A bundle that renders its UI may leave before and after PNG screenshots under
the gitignored ``.curie/ui-evidence/`` directory of the checkout, described by
``.curie/ui-evidence/manifest.json``. ``POST /v1/ui-evidence/index`` answers
with the validated index of that manifest: its status, each image's size,
sha256 and refusal, and whether the image is bound to this publication.
``POST /v1/ui-evidence/image`` serves the raw bytes of one image the index
accepted, only while they are unchanged.

Rules every read keeps:

1. Only regular files under ``.curie/ui-evidence/`` are read. Every path
   component is opened relative to its parent with ``O_NOFOLLOW``, so a
   symlink anywhere on the path is refused, never followed. Traversal is
   refused before anything is opened.
2. Reads are bounded: 1 MiB for the manifest, 10 MiB per image and 100 MiB of
   accepted images in total. Every image passes full PNG validation
   (signature, every chunk CRC, IHDR first with length 13, IEND last with
   length 0 and nothing after it).
3. Binding: a ``before`` image is bound only when the manifest's ``base_sha``
   is the request's prepared base P. An ``after`` image is bound only when the
   manifest's ``candidate_tree`` is the canonical candidate tree C that
   ``workspace_snapshot.build_candidate`` computes, or, when
   ``candidate_dirty`` is exactly false, when the tree of ``head_sha`` is C.
   ``head_sha`` is never consulted otherwise.
4. Evidence never touches the code publication: C is computed without a
   patch, nothing is written to the workspace, and the snapshot route, its
   patch, changed paths and ``MAX_PATCH_BYTES`` are unaffected. A problem with
   the evidence is a refusal in the index, never a failed request.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import stat
import time
import zlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from . import workspace_snapshot
from .workspace_snapshot import WorkspaceSnapshotError

__all__ = [
    "MAX_BUDGET_S",
    "MAX_IMAGE_BYTES",
    "MAX_MANIFEST_BYTES",
    "MAX_TOTAL_BYTES",
    "PNG_SIGNATURE",
    "EvidenceRefusal",
    "build_index",
    "parse_image_request",
    "parse_index_request",
    "read_image",
    "validate_png",
]

EVIDENCE_DIR = (b".curie", b"ui-evidence")
MANIFEST = b"manifest.json"
MAX_IMAGE_BYTES = 10 * 1024 * 1024
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_TOTAL_BYTES = 100 * 1024 * 1024
MAX_BUDGET_S = 120
MIN_BUDGET_S = 1
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
INDEX_VERSION = 1

_OID_RE = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_STATUSES = frozenset({"complete", "partial", "skipped", "failed"})
_STATES = frozenset({"before", "after"})
_INDEX_KEYS = frozenset({"base_sha", "budget_seconds"})
_IMAGE_KEYS = frozenset({"index", "sha256"})
_MAX_REASON = 1000
_MAX_LABEL = 200
_MAX_PATH = 512
_READ_CHUNK = 1024 * 1024
# PNG chunk lengths are at most 2^31 - 1.
_MAX_CHUNK = 2**31 - 1


class EvidenceRefusal(Exception):
    """An evidence request the runner answers with a coded refusal."""

    def __init__(self, code: str, status: int, message: str, *, refusal: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.status = status
        self.message = message
        self.refusal = refusal

    def body(self) -> dict[str, object]:
        body: dict[str, object] = {"error": self.message, "code": self.code}
        if self.refusal is not None:
            body["refusal"] = self.refusal
        return body


class _Refused(Exception):
    """One file the evidence rules refuse, carrying its refusal code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _invalid(message: str) -> EvidenceRefusal:
    return EvidenceRefusal("invalid_request", 400, message)


def parse_index_request(raw: bytes) -> tuple[str, float]:
    """``(base_sha, budget_seconds)`` from a body of exactly those two keys."""

    body = _decode_request(raw)
    if not isinstance(body, dict) or set(body) != _INDEX_KEYS:
        raise _invalid("body must be an object with base_sha and budget_seconds")
    base_sha = body["base_sha"]
    if not isinstance(base_sha, str) or not _OID_RE.fullmatch(base_sha):
        raise _invalid("base_sha must be a full lowercase hex object id")
    budget = body["budget_seconds"]
    if (
        isinstance(budget, bool)
        or not isinstance(budget, int | float)
        # Compared without float conversion: a huge integer would overflow it.
        # NaN and infinity fail this range too.
        or not MIN_BUDGET_S <= budget <= MAX_BUDGET_S
    ):
        raise _invalid(f"budget_seconds must be a number in [{MIN_BUDGET_S}, {MAX_BUDGET_S}]")
    return base_sha, float(budget)


def parse_image_request(raw: bytes) -> tuple[int, str]:
    """``(index, sha256)`` from a body of exactly those two keys."""

    body = _decode_request(raw)
    if not isinstance(body, dict) or set(body) != _IMAGE_KEYS:
        raise _invalid("body must be an object with index and sha256")
    index = body["index"]
    if type(index) is not int or index < 0:
        raise _invalid("index must be a non-negative integer")
    sha256 = body["sha256"]
    if not isinstance(sha256, str) or not _SHA256_RE.fullmatch(sha256):
        raise _invalid("sha256 must be a lowercase hex sha256 digest")
    return index, sha256


def _decode_request(raw: bytes) -> object:
    try:
        return json.loads(raw)
    except (ValueError, RecursionError):
        raise _invalid("body is not JSON") from None


# --- PNG validation -------------------------------------------------------------


def validate_png(data: bytes) -> str | None:
    """``None`` for a structurally whole PNG, else ``not_png`` or ``corrupt_png``.

    Checks the signature, that every chunk fits and carries a correct CRC32
    over its type and data, that the first chunk is an IHDR of length 13 and
    appears once, and that the last chunk is an IEND of length 0 with no byte
    after it. Pixel data is not decoded.
    """

    if not data.startswith(PNG_SIGNATURE):
        return "not_png"
    view = memoryview(data)
    offset = len(PNG_SIGNATURE)
    first = True
    while True:
        if offset + 12 > len(view):
            return "corrupt_png"
        length = int.from_bytes(view[offset : offset + 4], "big")
        kind = bytes(view[offset + 4 : offset + 8])
        end = offset + 12 + length
        if length > _MAX_CHUNK or end > len(view) or not kind.isalpha():
            return "corrupt_png"
        expected = int.from_bytes(view[end - 4 : end], "big")
        if zlib.crc32(view[offset + 4 : end - 4]) & 0xFFFFFFFF != expected:
            return "corrupt_png"
        if first != (kind == b"IHDR") or (first and length != 13):
            return "corrupt_png"
        if kind == b"IEND":
            return None if length == 0 and end == len(view) else "corrupt_png"
        first = False
        offset = end


# --- No-follow reads ------------------------------------------------------------


def _open_component(parent_fd: int, name: bytes, *, directory: bool) -> int:
    """Open ``name`` under ``parent_fd`` without following a symlink."""

    flags = (
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        if directory
        else os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_NOCTTY
    )
    try:
        return os.open(name, flags, dir_fd=parent_fd)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            raise _Refused("missing") from None
        if exc.errno in (errno.ELOOP, errno.ENOTDIR, errno.ENXIO, errno.EOPNOTSUPP):
            raise _Refused(_classify(parent_fd, name)) from None
        raise _Refused("missing") from None


def _classify(parent_fd: int, name: bytes) -> str:
    """Why ``name`` could not be opened: a symlink, or something not regular."""

    try:
        info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError:
        return "missing"
    return "symlink" if stat.S_ISLNK(info.st_mode) else "not_regular"


def _open_regular(root_fd: int, parts: list[bytes]) -> int:
    """Open the regular file at ``parts`` under ``root_fd``; every component no-follow."""

    fd = os.dup(root_fd)
    try:
        for part in parts[:-1]:
            child = _open_component(fd, part, directory=True)
            os.close(fd)
            fd = child
        leaf = _open_component(fd, parts[-1], directory=False)
    finally:
        os.close(fd)
    if not stat.S_ISREG(os.fstat(leaf).st_mode):
        os.close(leaf)
        raise _Refused("not_regular")
    return leaf


def _read_bounded(fd: int, limit: int, refusal: str) -> bytes:
    """At most ``limit`` bytes of ``fd``; ``refusal`` when the file is larger."""

    if os.fstat(fd).st_size > limit:
        raise _Refused(refusal)
    chunks: list[bytes] = []
    total = 0
    while chunk := os.read(fd, min(_READ_CHUNK, limit + 1 - total)):
        chunks.append(chunk)
        total += len(chunk)
        if total > limit:
            raise _Refused(refusal)
    return b"".join(chunks)


def _open_evidence_dir(workspace: Path) -> int:
    """The evidence directory's descriptor; the workspace root is runner configured."""

    try:
        root = os.open(workspace, os.O_RDONLY | os.O_DIRECTORY)
    except OSError:
        raise _Refused("missing") from None
    try:
        fd = os.dup(root)
        for part in EVIDENCE_DIR:
            try:
                child = _open_component(fd, part, directory=True)
            finally:
                os.close(fd)
            fd = child
        return fd
    finally:
        os.close(root)


# --- Manifest -------------------------------------------------------------------


@dataclass(frozen=True)
class _Manifest:
    status: str
    reason: str | None
    base_sha: str | None
    head_sha: str | None
    candidate_dirty: bool | None
    candidate_tree: str | None
    images: list[object]


def _optional_oid(document: Mapping[str, object], key: str) -> str | None:
    value = document.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not _OID_RE.fullmatch(value):
        raise _Refused("schema")
    return value


def _parse_manifest(raw: bytes) -> _Manifest:
    """The manifest's known fields; unknown keys are ignored."""

    try:
        document = json.loads(raw)
    except (ValueError, RecursionError):
        raise _Refused("not_json") from None
    if not isinstance(document, dict):
        raise _Refused("schema")
    version = document.get("version")
    if type(version) is not int or version != INDEX_VERSION:
        raise _Refused("schema")
    status = document.get("status")
    if not isinstance(status, str) or status not in _STATUSES:
        raise _Refused("schema")
    reason = document.get("reason")
    if reason is not None and (not isinstance(reason, str) or len(reason) > _MAX_REASON):
        raise _Refused("schema")
    if status != "complete" and not reason:
        raise _Refused("schema")
    candidate_dirty = document.get("candidate_dirty")
    if candidate_dirty is not None and not isinstance(candidate_dirty, bool):
        raise _Refused("schema")
    images = document.get("images", [])
    if not isinstance(images, list):
        raise _Refused("schema")
    return _Manifest(
        status=status,
        reason=reason,
        base_sha=_optional_oid(document, "base_sha"),
        head_sha=_optional_oid(document, "head_sha"),
        candidate_dirty=candidate_dirty,
        candidate_tree=_optional_oid(document, "candidate_tree"),
        images=images,
    )


def _read_manifest(evidence_fd: int) -> _Manifest:
    fd = _open_regular(evidence_fd, [MANIFEST])
    try:
        raw = _read_bounded(fd, MAX_MANIFEST_BYTES, "too_large")
    finally:
        os.close(fd)
    return _parse_manifest(raw)


# --- Image entries --------------------------------------------------------------


@dataclass(frozen=True)
class _Entry:
    path: str | None
    label: str | None
    state: str | None


def _entry_fields(raw: object) -> _Entry:
    """The entry's well-typed fields, each ``None`` when missing or ill-typed."""

    if not isinstance(raw, dict):
        return _Entry(None, None, None)
    path, label, state = raw.get("path"), raw.get("label"), raw.get("state")
    return _Entry(
        path=path if isinstance(path, str) and len(path) <= _MAX_PATH else None,
        label=label if isinstance(label, str) and len(label) <= _MAX_LABEL else None,
        state=state if isinstance(state, str) and state in _STATES else None,
    )


def _entry_parts(raw: object) -> list[bytes]:
    """The entry's path as components under the evidence directory, or refuse it."""

    if not isinstance(raw, dict):
        raise _Refused("invalid_entry")
    path, label, state = raw.get("path"), raw.get("label"), raw.get("state")
    if not isinstance(path, str) or not isinstance(label, str) or len(label) > _MAX_LABEL:
        raise _Refused("invalid_entry")
    if not isinstance(state, str) or state not in _STATES:
        raise _Refused("invalid_entry")
    if not path or len(path) > _MAX_PATH or path.startswith("/") or "\\" in path or "\x00" in path:
        raise _Refused("traversal")
    segments = path.split("/")
    if any(segment in ("", ".", "..") for segment in segments):
        raise _Refused("traversal")
    try:
        return [segment.encode("utf-8") for segment in segments]
    except UnicodeEncodeError:
        raise _Refused("invalid_entry") from None


def _read_image_entry(evidence_fd: int, raw: object) -> bytes:
    """The validated PNG bytes of one manifest entry, or its refusal."""

    fd = _open_regular(evidence_fd, _entry_parts(raw))
    try:
        data = _read_bounded(fd, MAX_IMAGE_BYTES, "too_large")
    finally:
        os.close(fd)
    refusal = validate_png(data)
    if refusal is not None:
        raise _Refused(refusal)
    return data


# --- Index ----------------------------------------------------------------------


def _empty_index(base_sha: str, manifest: str, refusal: str | None) -> dict[str, object]:
    return {
        "version": INDEX_VERSION,
        "manifest": manifest,
        "manifest_refusal": refusal,
        "status": None,
        "reason": None,
        "base_sha": base_sha,
        "manifest_base_sha": None,
        "manifest_head_sha": None,
        "candidate_dirty": None,
        "manifest_candidate_tree": None,
        "candidate_tree": None,
        "candidate_error": None,
        "head_tree": None,
        "images": [],
    }


def _head_tree(workspace: Path, head_sha: str, deadline: float) -> str | None:
    """The tree id of ``head_sha`` in the checkout, or ``None`` when it does not resolve."""

    try:
        result = workspace_snapshot._git(  # noqa: SLF001 - the snapshot's own git policy
            workspace,
            "rev-parse",
            "--verify",
            "--quiet",
            "--end-of-options",
            f"{head_sha}^{{tree}}",
            check=False,
            deadline=deadline,
        )
    except WorkspaceSnapshotError:
        return None
    if result.returncode != 0:
        return None
    tree = result.stdout.decode("ascii", errors="replace").strip()
    return tree if _OID_RE.fullmatch(tree) else None


def build_index(workspace: Path, base_sha: str, *, deadline: float) -> dict[str, object]:
    """The validated evidence index for prepared base ``base_sha``.

    ``deadline`` is a ``time.monotonic()`` value. It bounds the candidate tree
    computation and the image reads; an entry not reached in time is refused
    as ``timeout``. Every evidence problem is reported in the result.
    """

    try:
        evidence_fd = _open_evidence_dir(workspace)
    except _Refused as refused:
        if refused.code == "missing":
            return _empty_index(base_sha, "absent", None)
        return _empty_index(base_sha, "invalid", refused.code)
    try:
        try:
            manifest = _read_manifest(evidence_fd)
        except _Refused as refused:
            if refused.code == "missing":
                return _empty_index(base_sha, "absent", None)
            return _empty_index(base_sha, "invalid", refused.code)
        index = _empty_index(base_sha, "valid", None)
        index.update(
            status=manifest.status,
            reason=manifest.reason,
            manifest_base_sha=manifest.base_sha,
            manifest_head_sha=manifest.head_sha,
            candidate_dirty=manifest.candidate_dirty,
            manifest_candidate_tree=manifest.candidate_tree,
        )
        candidate_tree: str | None = None
        try:
            candidate_tree = workspace_snapshot.build_candidate(
                workspace, base_sha, render_patch=False, deadline=deadline
            ).tree
        except WorkspaceSnapshotError as exc:
            index["candidate_error"] = str(exc)
        index["candidate_tree"] = candidate_tree
        head_tree: str | None = None
        # head_sha is consulted only for a capture that declares itself clean.
        if manifest.candidate_dirty is False and manifest.head_sha is not None:
            head_tree = _head_tree(workspace, manifest.head_sha, deadline)
        index["head_tree"] = head_tree
        bound_states = {
            "before": manifest.base_sha == base_sha,
            "after": candidate_tree is not None
            and (
                manifest.candidate_tree == candidate_tree
                or (manifest.candidate_dirty is False and head_tree == candidate_tree)
            ),
        }
        index["images"] = _index_images(evidence_fd, manifest.images, bound_states, deadline)
        return index
    finally:
        os.close(evidence_fd)


def _index_images(
    evidence_fd: int,
    entries: list[object],
    bound_states: Mapping[str, bool],
    deadline: float,
) -> list[dict[str, object]]:
    images: list[dict[str, object]] = []
    total = 0
    for ordinal, raw in enumerate(entries):
        fields = _entry_fields(raw)
        image: dict[str, object] = {
            "ordinal": ordinal,
            "path": fields.path,
            "label": fields.label,
            "state": fields.state,
            "size": None,
            "sha256": None,
            "refusal": None,
            "bound": False,
        }
        images.append(image)
        if time.monotonic() >= deadline:
            image["refusal"] = "timeout"
            continue
        try:
            data = _read_image_entry(evidence_fd, raw)
        except _Refused as refused:
            image["refusal"] = refused.code
            continue
        image["size"] = len(data)
        image["sha256"] = hashlib.sha256(data).hexdigest()
        if fields.state is None or not bound_states[fields.state]:
            image["refusal"] = "unbound"
            continue
        image["bound"] = True
        if total + len(data) > MAX_TOTAL_BYTES:
            image["refusal"] = "total_limit"
            continue
        total += len(data)
    return images


def read_image(workspace: Path, index: int, sha256: str) -> bytes:
    """The bytes of manifest entry ``index``, validated again and unchanged.

    Raises ``EvidenceRefusal``: 404 ``no_such_image`` when there is no such
    entry, 409 ``image_refused`` naming the refusal when the entry fails the
    file or PNG rules now, and 409 ``image_changed`` when its sha256 is not
    ``sha256``. Binding is the index route's verdict and is not repeated here.
    """

    no_such = EvidenceRefusal("no_such_image", 404, "no evidence image at that index")
    try:
        evidence_fd = _open_evidence_dir(workspace)
    except _Refused:
        raise no_such from None
    try:
        try:
            manifest = _read_manifest(evidence_fd)
        except _Refused:
            raise no_such from None
        if index >= len(manifest.images):
            raise no_such
        try:
            data = _read_image_entry(evidence_fd, manifest.images[index])
        except _Refused as refused:
            raise EvidenceRefusal(
                "image_refused", 409, "the evidence image is refused", refusal=refused.code
            ) from None
    finally:
        os.close(evidence_fd)
    if hashlib.sha256(data).hexdigest() != sha256:
        raise EvidenceRefusal("image_changed", 409, "the evidence image changed since indexing")
    return data
