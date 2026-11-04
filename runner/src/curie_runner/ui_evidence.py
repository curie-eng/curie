"""Optional UI screenshot evidence from the managed workspace.

A bundle may leave before and after PNG screenshots under the gitignored
``.curie/ui-evidence/`` directory, described by its ``manifest.json``.
``POST /v1/ui-evidence/index`` answers with the validated index of that
manifest; ``POST /v1/ui-evidence/image`` serves one accepted image's raw bytes
only while they are unchanged.

Rules every read keeps:

1. Only regular files under ``.curie/ui-evidence/`` are read. Every path
   component is opened relative to its parent with ``O_NOFOLLOW``, so a
   symlink anywhere on the path is refused, never followed. Traversal is
   refused before anything is opened.
2. Reads are bounded: 1 MiB for the manifest, 10 MiB per image and 100 MiB of
   accepted images in total. Every image passes full PNG validation.
3. Binding: a ``before`` image is bound only when the manifest's ``base_sha``
   is the request's prepared base P. An ``after`` image is bound only when the
   manifest's ``candidate_tree`` is the canonical candidate tree C that
   ``workspace_snapshot.build_candidate`` computes, or, when
   ``candidate_dirty`` is exactly false, when the tree of ``head_sha`` is C.
   ``head_sha`` is never consulted otherwise.
4. Evidence never touches the code publication: C is computed without a
   patch and nothing is written to the workspace. A problem with the evidence
   is a refusal in the index, never a failed request.
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
from pathlib import Path
from typing import Any

from . import workspace_snapshot
from .workspace_snapshot import WorkspaceSnapshotError

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
        self.refusal = refusal

    def body(self) -> dict[str, object]:
        body: dict[str, object] = {"error": str(self), "code": self.code}
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


def _request(raw: bytes, keys: frozenset[str]) -> dict[str, Any]:
    try:
        body = json.loads(raw)
    except (ValueError, RecursionError):
        raise _invalid("body is not JSON") from None
    if not isinstance(body, dict) or set(body) != keys:
        raise _invalid(f"body must be an object with {' and '.join(sorted(keys))}")
    return body


def parse_index_request(raw: bytes) -> tuple[str, float]:
    body = _request(raw, frozenset({"base_sha", "budget_seconds"}))
    base_sha, budget = body["base_sha"], body["budget_seconds"]
    if not isinstance(base_sha, str) or not _OID_RE.fullmatch(base_sha):
        raise _invalid("base_sha must be a full lowercase hex object id")
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
    body = _request(raw, frozenset({"index", "sha256"}))
    index, sha256 = body["index"], body["sha256"]
    if type(index) is not int or index < 0:
        raise _invalid("index must be a non-negative integer")
    if not isinstance(sha256, str) or not _SHA256_RE.fullmatch(sha256):
        raise _invalid("sha256 must be a lowercase hex sha256 digest")
    return index, sha256


def validate_png(data: bytes) -> str | None:
    """``None`` for a structurally whole PNG, else ``not_png`` or ``corrupt_png``.

    Checks the signature, every chunk's bounds and CRC32, that the first chunk
    is the only IHDR with length 13, and that the last is an IEND of length 0
    with no byte after it. Pixel data is not decoded.
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
        if exc.errno not in (errno.ELOOP, errno.ENOTDIR, errno.ENXIO, errno.EOPNOTSUPP):
            raise _Refused("missing") from None
    try:
        info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError:
        raise _Refused("missing") from None
    raise _Refused("symlink" if stat.S_ISLNK(info.st_mode) else "not_regular")


def _open_dirs(parent_fd: int, parts: tuple[bytes, ...] | list[bytes]) -> int:
    """The directory at ``parts`` under ``parent_fd``, every component no-follow."""

    fd = os.dup(parent_fd)
    for part in parts:
        try:
            child = _open_component(fd, part, directory=True)
        finally:
            os.close(fd)
        fd = child
    return fd


def _open_regular(root_fd: int, parts: list[bytes]) -> int:
    fd = _open_dirs(root_fd, parts[:-1])
    try:
        leaf = _open_component(fd, parts[-1], directory=False)
    finally:
        os.close(fd)
    if not stat.S_ISREG(os.fstat(leaf).st_mode):
        os.close(leaf)
        raise _Refused("not_regular")
    return leaf


def _read_bounded(root_fd: int, parts: list[bytes], limit: int) -> bytes:
    """The regular file at ``parts``, or ``too_large`` past ``limit`` bytes."""

    fd = _open_regular(root_fd, parts)
    try:
        if os.fstat(fd).st_size > limit:
            raise _Refused("too_large")
        chunks: list[bytes] = []
        total = 0
        while chunk := os.read(fd, min(_READ_CHUNK, limit + 1 - total)):
            chunks.append(chunk)
            total += len(chunk)
            if total > limit:
                raise _Refused("too_large")
        return b"".join(chunks)
    finally:
        os.close(fd)


# --- Manifest -------------------------------------------------------------------


def _parse_manifest(raw: bytes) -> dict[str, Any]:
    """The manifest's known fields under their index names; unknown keys are ignored."""

    try:
        document = json.loads(raw)
    except (ValueError, RecursionError):
        raise _Refused("not_json") from None
    if not isinstance(document, dict):
        raise _Refused("schema")
    get = document.get
    version, status, reason = get("version"), get("status"), get("reason")
    dirty, images = get("candidate_dirty"), get("images", [])
    oids = {f"manifest_{key}": get(key) for key in ("base_sha", "head_sha", "candidate_tree")}
    if (
        type(version) is not int
        or version != INDEX_VERSION
        or not isinstance(status, str)
        or status not in _STATUSES
        or (reason is not None and (not isinstance(reason, str) or len(reason) > _MAX_REASON))
        or (status != "complete" and not reason)
        or (dirty is not None and not isinstance(dirty, bool))
        or not isinstance(images, list)
        or any(
            oid is not None and (not isinstance(oid, str) or not _OID_RE.fullmatch(oid))
            for oid in oids.values()
        )
    ):
        raise _Refused("schema")
    return {"status": status, "reason": reason, "candidate_dirty": dirty, "images": images, **oids}


def _open_manifest(workspace: Path) -> tuple[int, dict[str, Any]]:
    """The evidence directory's descriptor and its parsed manifest."""

    # The workspace root itself is runner configured, not evidence.
    try:
        root = os.open(workspace, os.O_RDONLY | os.O_DIRECTORY)
    except OSError:
        raise _Refused("missing") from None
    try:
        evidence_fd = _open_dirs(root, EVIDENCE_DIR)
    finally:
        os.close(root)
    try:
        raw = _read_bounded(evidence_fd, [MANIFEST], MAX_MANIFEST_BYTES)
        return evidence_fd, _parse_manifest(raw)
    except BaseException:
        os.close(evidence_fd)
        raise


# --- Image entries --------------------------------------------------------------


def _entry_fields(raw: object) -> dict[str, str | None]:
    """The entry's well-typed fields, each ``None`` when missing or ill-typed."""

    if not isinstance(raw, dict):
        return {"path": None, "label": None, "state": None}
    path, label, state = raw.get("path"), raw.get("label"), raw.get("state")
    return {
        "path": path if isinstance(path, str) and len(path) <= _MAX_PATH else None,
        "label": label if isinstance(label, str) and len(label) <= _MAX_LABEL else None,
        "state": state if isinstance(state, str) and state in _STATES else None,
    }


def _read_image_entry(evidence_fd: int, raw: object) -> bytes:
    """The validated PNG bytes of one manifest entry, or its refusal."""

    path = raw.get("path") if isinstance(raw, dict) else None
    fields = _entry_fields(raw)
    if not isinstance(path, str) or fields["label"] is None or fields["state"] is None:
        raise _Refused("invalid_entry")
    segments = path.split("/")
    if (
        not path
        or len(path) > _MAX_PATH
        or path.startswith("/")
        or "\\" in path
        or "\x00" in path
        or any(segment in ("", ".", "..") for segment in segments)
    ):
        raise _Refused("traversal")
    try:
        parts = [segment.encode("utf-8") for segment in segments]
    except UnicodeEncodeError:
        raise _Refused("invalid_entry") from None
    data = _read_bounded(evidence_fd, parts, MAX_IMAGE_BYTES)
    refusal = validate_png(data)
    if refusal is not None:
        raise _Refused(refusal)
    return data


# --- Index ----------------------------------------------------------------------


def _empty_index(base_sha: str, manifest: str, refusal: str | None) -> dict[str, Any]:
    index: dict[str, Any] = {"version": INDEX_VERSION, "manifest": manifest, "base_sha": base_sha}
    index.update(manifest_refusal=refusal, status=None, reason=None, manifest_base_sha=None)
    index.update(manifest_head_sha=None, candidate_dirty=None, manifest_candidate_tree=None)
    index.update(candidate_tree=None, candidate_error=None, head_tree=None, images=[])
    return index


def _head_tree(workspace: Path, head_sha: str, deadline: float) -> str | None:
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
    tree = result.stdout.decode("ascii", errors="replace").strip()
    return tree if result.returncode == 0 and _OID_RE.fullmatch(tree) else None


def build_index(workspace: Path, base_sha: str, *, deadline: float) -> dict[str, object]:
    """The validated evidence index for prepared base ``base_sha``.

    ``deadline`` (``time.monotonic()``) bounds the candidate tree computation
    and the image reads; an entry not reached in time is refused ``timeout``.
    """

    try:
        evidence_fd, manifest = _open_manifest(workspace)
    except _Refused as refused:
        if refused.code == "missing":
            return _empty_index(base_sha, "absent", None)
        return _empty_index(base_sha, "invalid", refused.code)
    try:
        index = _empty_index(base_sha, "valid", None)
        index.update(manifest)
        try:
            index["candidate_tree"] = workspace_snapshot.build_candidate(
                workspace, base_sha, render_patch=False, deadline=deadline
            ).tree
        except WorkspaceSnapshotError as exc:
            index["candidate_error"] = str(exc)
        candidate_tree = index["candidate_tree"]
        clean = manifest["candidate_dirty"] is False
        # head_sha is consulted only for a capture that declares itself clean.
        if clean and manifest["manifest_head_sha"] is not None:
            index["head_tree"] = _head_tree(workspace, manifest["manifest_head_sha"], deadline)
        bound_states = {
            "before": manifest["manifest_base_sha"] == base_sha,
            "after": candidate_tree is not None
            and (
                manifest["manifest_candidate_tree"] == candidate_tree
                or (clean and index["head_tree"] == candidate_tree)
            ),
        }
        images: list[dict[str, object]] = []
        total = 0
        for ordinal, raw in enumerate(manifest["images"]):
            fields = _entry_fields(raw)
            image: dict[str, object] = {"ordinal": ordinal, **fields, "size": None}
            image.update(sha256=None, refusal=None, bound=False)
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
            state = fields["state"]
            if state is None or not bound_states[state]:
                image["refusal"] = "unbound"
                continue
            image["bound"] = True
            if total + len(data) > MAX_TOTAL_BYTES:
                image["refusal"] = "total_limit"
                continue
            total += len(data)
        index["images"] = images
        return index
    finally:
        os.close(evidence_fd)


def read_image(workspace: Path, index: int, sha256: str) -> bytes:
    """The bytes of manifest entry ``index``, validated again and unchanged.

    Binding is the index route's verdict and is not repeated here.
    """

    no_such = EvidenceRefusal("no_such_image", 404, "no evidence image at that index")
    try:
        evidence_fd, manifest = _open_manifest(workspace)
    except _Refused:
        raise no_such from None
    try:
        if index >= len(manifest["images"]):
            raise no_such
        data = _read_image_entry(evidence_fd, manifest["images"][index])
    except _Refused as refused:
        raise EvidenceRefusal(
            "image_refused", 409, "the evidence image is refused", refusal=refused.code
        ) from None
    finally:
        os.close(evidence_fd)
    if hashlib.sha256(data).hexdigest() != sha256:
        raise EvidenceRefusal("image_changed", 409, "the evidence image changed since indexing")
    return data
