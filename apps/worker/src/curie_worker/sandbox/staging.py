"""Claim input staging shared by the substrates that stage on the worker.

The Kubernetes substrate stages a claim's inputs in init containers. The
Docker and static-pool substrates have none, so the worker itself fetches the
immutable bundle and extracts it with ``extract_bundle``'s bounds, fetches and
verifies the signed workspace archive, and redeems attachment references, into
a private temporary tree per claim attempt. Each substrate then moves that
tree to its runner its own way (a claim-owned Docker volume, or the staging
archive a shell supervisor unpacks) and releases it with
``_cleanup_staged_inputs``.
"""

from __future__ import annotations

import base64
import hashlib
import http.client
import json
import os
import shutil
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ..attachments import (
    ATTACHMENTS_REF_ENV,
    AttachmentLimits,
    AttachmentRef,
    AttachmentResolutionError,
    AttachmentTooLargeError,
    decode_attachment_refs,
    unique_attachment_leaf,
)
from ..binding import BUNDLE_REF_ENV, CREDENTIALS_ENV, PLUGIN_DIR_ENV
from ..bundle_store import BundleReader, extract_bundle
from ..workspace import (
    WORKSPACE_REF_ENV,
    WORKSPACE_SHA256_ENV,
    WorkspaceLimits,
    WorkspacePreparationError,
    WorkspaceRef,
    validate_workspace_archive,
)

#: The overall deadline for redeeming one boot's attachment references,
#: current and earlier files together, measured on ``time.monotonic``. Before
#: every read the socket timeout is what is left of it, and the deadline is
#: checked after every read. The twin of the chart's
#: ``agentSandbox.runner.attachments.fetchTimeoutSeconds`` default, which the
#: ``attachments-init`` program applies the same way (ADR 0205 decision 6), and
#: inside the worker's claim timeout. A local constant rather than an
#: AttachmentLimits field: the docker tiers have no operator envelope for it.
_ATTACHMENT_FETCH_DEADLINE_S = 45.0

#: The hidden file in the mount root recording every reference's outcome, read
#: by the runner (ADR 0205 decision 8). The ``.curie-`` prefix is reserved: no
#: attachment may be named into it.
_ATTACHMENT_STATUS_FILE = ".curie-attachments-status.json"
_ATTACHMENT_RESERVED_PREFIX = ".curie-"
#: The longest leaf a Linux filesystem holds, in bytes.
_ATTACHMENT_NAME_MAX_BYTES = 255


class _AttachmentUnavailable(Exception):
    """One reference could not be fetched; ``reason`` is the status code."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse every redirect, as the ``attachments-init`` opener does.

    A redirect carries the fetch to a host the store named rather than one the
    worker chose, so it surfaces as the 3xx ``HTTPError`` instead.
    """

    def redirect_request(  # type: ignore[override]
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Mapping[str, str],
        newurl: str,
    ) -> None:
        return None


def _attachment_currency(encoded_refs: str) -> tuple[bool, ...] | None:
    """Per entry, whether it is on the current message (``"c"`` on the wire).

    ``decode_attachment_refs`` has already validated the payload, so this only
    reads the flag it does not carry: ``"c": 0`` is an earlier file. ``None``
    means no entry carries ``"c"``: the payload is from a worker that predates
    ADR 0205, and the whole of it keeps the pre-ADR rules.
    """

    raw = base64.urlsafe_b64decode(encoded_refs + "=" * (-len(encoded_refs) % 4))
    entries = json.loads(raw)
    if not any("c" in entry for entry in entries):
        return None
    return tuple(entry.get("c", 1) != 0 for entry in entries)


def _attachment_name_is_clean(name: str) -> bool:
    """The same rule the ``attachments-init`` program applies, name for name."""

    return (
        bool(name.strip())
        and name not in (".", "..")
        and os.path.basename(name) == name
        and "\0" not in name
        and not name.startswith(_ATTACHMENT_RESERVED_PREFIX)
    )


def _legacy_attachment_leaf(name: str) -> str:
    """The pre-ADR 0205 cleaning, the twin of the init program's legacy branch."""

    return os.path.basename(name.replace("\\", "/").strip())


def _bound_read(response: Any, deadline: float) -> None:
    """Set the next read's socket timeout to what is left of the deadline."""

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("past the attachment fetch deadline")
    sock = getattr(getattr(getattr(response, "fp", None), "raw", None), "_sock", None)
    if sock is not None:
        sock.settimeout(remaining)


class NoAttachments(Exception):
    """The claim carried a reference that decoded to zero files.

    Not an error: a turn whose attachment list is empty must produce exactly
    today's container spec, with no mount and no staged directory, so the empty
    case is indistinguishable from a turn that never carried the key at all.
    """


# Env keys the worker sets explicitly or forwards specially, so the generic
# value loop must not also emit them: the plugin dir and sandbox id are set
# explicitly, the bundle ref names a RustFS object the worker already fetched,
# workspace ref/digest name the archive the worker already fetched and mounted,
# and the credential is forwarded by name (never as a value in the argv).
# CURIE_BUNDLE_VERSION is deliberately not in this set: it is the agent-readable
# version_label and must reach the child env (#2174).
# The attachment capability is redeemed HERE, by this driver, exactly as
# the bundle ref is. Forwarding it would hand the sandbox a live signed
# url it has no reason to hold; Kubernetes scopes the same key to its
# init container and keeps it off the runner, and the two substrates
# must not disagree about what the agent can reach.
WORKER_OWNED_ENV = frozenset(
    {
        BUNDLE_REF_ENV,
        PLUGIN_DIR_ENV,
        "CURIE_SANDBOX_ID",
        CREDENTIALS_ENV,
        WORKSPACE_REF_ENV,
        WORKSPACE_SHA256_ENV,
        ATTACHMENTS_REF_ENV,
    }
)


class ClaimInputStaging:
    """Worker-side staging of one claim attempt's bundle, workspace and attachments.

    A substrate calls ``_init_staging`` from its constructor. Every staged
    tree is keyed by the claim attempt, so a losing creator never removes a
    winner's inputs.
    """

    _bundles: BundleReader
    _bundle_max_uncompressed_bytes: int
    _bundle_max_compression_ratio: float
    _bundle_max_members: int
    _workspace_limits: WorkspaceLimits
    _attachment_limits: AttachmentLimits
    _bundle_dirs: dict[str, str]
    _workspace_dirs: dict[str, str]
    _attachment_dirs: dict[str, str]

    def _init_staging(
        self,
        *,
        bundle_store: BundleReader,
        bundle_max_uncompressed_bytes: int,
        bundle_max_compression_ratio: float,
        bundle_max_members: int,
        workspace_limits: WorkspaceLimits | None,
        attachment_limits: AttachmentLimits | None,
    ) -> None:
        self._bundles = bundle_store
        # Extraction bounds (ADR-0059 decision 3), forwarded to
        # ``extract_bundle`` at claim time. ``run.py`` passes the
        # operator-configured ``WorkerConfig`` values.
        self._bundle_max_uncompressed_bytes = bundle_max_uncompressed_bytes
        self._bundle_max_compression_ratio = bundle_max_compression_ratio
        self._bundle_max_members = bundle_max_members
        self._bundle_dirs = {}
        self._workspace_dirs = {}
        self._workspace_limits = workspace_limits or WorkspaceLimits()
        self._attachment_dirs = {}
        self._attachment_limits = attachment_limits or AttachmentLimits()

    def _cleanup_staged_inputs(self, name: str) -> None:
        self._cleanup_bundle(name)
        self._cleanup_workspace(name)
        self._cleanup_attachments(name)

    def _prepare_bundle(self, name: str, ref: str) -> str:
        data = self._bundles.get(ref)
        tmp = tempfile.mkdtemp(prefix="curie-bundle-")
        try:
            root = extract_bundle(
                data,
                Path(tmp),
                max_uncompressed_bytes=self._bundle_max_uncompressed_bytes,
                max_compression_ratio=self._bundle_max_compression_ratio,
                max_members=self._bundle_max_members,
            )
            # The mount is read-only and the runner is non-root (uid 1000), so the
            # staged bundle must be group/other readable+traversable; mkdtemp
            # defaults to 0700, which the non-root runner cannot enter.
            os.chmod(tmp, 0o755)
            for dirpath, dirnames, filenames in os.walk(tmp):
                for d in dirnames:
                    os.chmod(os.path.join(dirpath, d), 0o755)
                for f in filenames:
                    p = os.path.join(dirpath, f)
                    os.chmod(p, os.stat(p).st_mode | 0o044)
        except Exception:
            shutil.rmtree(tmp, ignore_errors=True)
            raise
        self._bundle_dirs[name] = tmp
        return str(root)

    def _cleanup_bundle(self, name: str) -> None:
        tmp = self._bundle_dirs.pop(name, None)
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)

    def _prepare_workspace(self, name: str, encoded_ref: str, expected_sha256: str) -> str:
        """Fetch one signed object, verify it, and safely materialize /workspace."""

        reference = WorkspaceRef.decode(encoded_ref)
        if reference.expires_in_seconds <= 0:
            raise WorkspacePreparationError("workspace-fetch", "workspace reference expired")
        if expected_sha256 != reference.sha256:
            raise WorkspacePreparationError(
                "workspace-fetch", "claim digest disagrees with signed workspace reference"
            )
        tmp = tempfile.mkdtemp(prefix="curie-workspace-")
        archive_path = Path(tmp) / ".workspace-archive"
        root = Path(tmp) / "checkout"
        root.mkdir(mode=0o700)
        digest = hashlib.sha256()
        total = 0
        request = urllib.request.Request(reference.url, method="GET")
        try:
            with (
                urllib.request.urlopen(
                    request, timeout=self._workspace_limits.clone_timeout_seconds
                ) as response,
                archive_path.open("wb") as output,
            ):
                while chunk := response.read(1024 * 1024):
                    total += len(chunk)
                    if total > self._workspace_limits.max_archive_bytes:
                        raise WorkspacePreparationError(
                            "workspace-fetch", "workspace archive exceeds compressed-size limit"
                        )
                    digest.update(chunk)
                    output.write(chunk)
            if digest.hexdigest() != reference.sha256:
                raise WorkspacePreparationError(
                    "workspace-fetch", "workspace archive digest mismatch"
                )
            with archive_path.open("rb") as source:
                validate_workspace_archive(
                    iter(lambda: source.read(1024 * 1024), b""),
                    limits=self._workspace_limits,
                )
            with tarfile.open(archive_path, mode="r:*") as archive:
                archive.extractall(root, filter="data")
            archive_path.unlink(missing_ok=True)
        except Exception:
            shutil.rmtree(tmp, ignore_errors=True)
            raise
        self._workspace_dirs[name] = tmp
        return str(root)

    def _cleanup_workspace(self, name: str) -> None:
        tmp = self._workspace_dirs.pop(name, None)
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)

    def _prepare_attachments(self, name: str, encoded_refs: str) -> str:
        """Redeem the signed references and materialize a read-only mount root.

        The Kubernetes twin of this is the ``attachments-init`` container, and
        the two are held to one outcome per payload by
        ``tests/vectors/attachment-init-outcomes.json``. ``decode_attachment_refs``
        has already refused a non-HTTP(S) url and a digest that is not real hex.
        What is left (ADR 0205 decisions 4, 6 and 8):

        * every name is written exactly as the worker recorded it; one that is
          blank, not a clean leaf, in the reserved ``.curie-`` prefix, or a
          repeat refuses the boot before anything is fetched;
        * the current message's files are fetched first and are all or
          nothing, matching ``AttachmentCoordinator.resolve``: a partial set is
          indistinguishable to the agent from a complete one;
        * an earlier file that has expired, is answered with an HTTP error or a
          redirect, outgrows the cap, has a name the filesystem cannot hold,
          times out, or is reached after the overall deadline is skipped and
          recorded unavailable with its reason;
        * a digest mismatch refuses the boot either way, because that is
          integrity, not availability;
        * the outcome of every entry lands in the hidden status file, renamed
          into the root before the root is mounted read-only.

        A payload with no ``"c"`` on any entry is from a worker that predates
        ADR 0205 and keeps the pre-ADR rules whole: every entry current, the
        name cleaned to its basename, a repeat given a numeric suffix
        (``unique_attachment_leaf``), and no status file.

        Every refusal is an ``AttachmentResolutionError`` and removes the whole
        staged directory.
        """

        refs = decode_attachment_refs(encoded_refs)
        if not refs:
            raise NoAttachments
        try:
            currency = _attachment_currency(encoded_refs)
        except (TypeError, ValueError, AttributeError) as exc:
            raise AttachmentResolutionError("reference", "invalid attachment reference") from exc
        legacy = currency is None
        current = (True,) * len(refs) if currency is None else currency
        leaves: list[str] = []
        for ref in refs:
            if legacy:
                leaf = _legacy_attachment_leaf(ref.name)
                if not leaf or leaf in (".", ".."):
                    raise AttachmentResolutionError(
                        "attachments-fetch",
                        f"name {ref.name!r} is unusable as a filename",
                    )
                leaf = unique_attachment_leaf(leaf, leaves)
            else:
                leaf = ref.name
                if not _attachment_name_is_clean(leaf):
                    raise AttachmentResolutionError(
                        "attachments-fetch",
                        f"name {ref.name!r} is unusable as a filename",
                    )
                if leaf in leaves:
                    raise AttachmentResolutionError(
                        "attachments-fetch",
                        f"name {ref.name!r} is duplicated",
                    )
            leaves.append(leaf)
        deadline = time.monotonic() + _ATTACHMENT_FETCH_DEADLINE_S
        tmp = tempfile.mkdtemp(prefix="curie-attachments-")
        root = Path(tmp) / "attachments"
        root.mkdir(mode=0o755)
        outcome: dict[str, tuple[str, str | None]] = {}
        # The current message's files first, whatever the payload order, so a
        # slow earlier file can never starve the ones the person just sent.
        order = sorted(range(len(refs)), key=lambda index: not current[index])
        try:
            for index in order:
                ref, leaf = refs[index], leaves[index]
                try:
                    self._fetch_attachment(ref, root / leaf, deadline, current=current[index])
                except _AttachmentUnavailable as exc:
                    if current[index]:
                        raise AttachmentResolutionError(
                            "attachments-fetch",
                            f"{ref.name!r} is unavailable: {exc.reason}",
                        ) from exc
                    outcome[leaf] = ("unavailable", exc.reason)
                else:
                    outcome[leaf] = ("ok", None)
            if not legacy:
                rows = [
                    {"name": leaf, "status": outcome[leaf][0], "reason": outcome[leaf][1]}
                    for leaf in leaves
                ]
                # Written beside the root and renamed in, so the runner reads it
                # whole or not at all; o+r because the runner is uid 1000.
                pending = Path(tmp) / "status.part"
                pending.write_text(json.dumps({"v": 1, "files": rows}))
                os.chmod(pending, 0o644)
                os.replace(pending, root / _ATTACHMENT_STATUS_FILE)
        except Exception:
            shutil.rmtree(tmp, ignore_errors=True)
            raise
        self._attachment_dirs[name] = tmp
        return str(root)

    def _fetch_attachment(
        self, ref: AttachmentRef, destination: Path, deadline: float, *, current: bool
    ) -> None:
        """Fetch one reference to ``destination``, verified, or say why it is unavailable.

        Raises ``_AttachmentUnavailable`` for what decision 6 lets an earlier
        file be skipped for, and ``AttachmentResolutionError`` for what refuses
        any boot (a digest mismatch, a name escaping the root, and for a current
        file the size cap, kept as ``AttachmentTooLargeError``). Nothing partial
        is left behind either way.
        """

        if ref.expires_in_seconds <= 0:
            raise _AttachmentUnavailable("expired")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise _AttachmentUnavailable("deadline")
        # A name the filesystem cannot hold is a file that cannot be delivered.
        if len(destination.name.encode("utf-8", "surrogateescape")) > _ATTACHMENT_NAME_MAX_BYTES:
            raise _AttachmentUnavailable("fetch_failed")
        root = destination.parent
        if destination.resolve().parent != root.resolve():
            raise AttachmentResolutionError(
                "attachments-fetch",
                f"name {ref.name!r} escapes the mount root",
            )
        digest = hashlib.sha256()
        total = 0
        request = urllib.request.Request(ref.url, method="GET")
        opener = urllib.request.build_opener(_NoRedirect())
        try:
            try:
                with (
                    opener.open(request, timeout=remaining) as response,
                    destination.open("wb") as output,
                ):
                    # read1 where the response has it: read(n) waits for n
                    # bytes, so a store dripping a byte at a time would hold it
                    # past the deadline.
                    read = getattr(response, "read1", None) or response.read
                    while True:
                        _bound_read(response, deadline)
                        chunk = read(self._attachment_limits.read_chunk_bytes)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > self._attachment_limits.max_file_bytes:
                            if current:
                                raise AttachmentTooLargeError(
                                    ref.name, self._attachment_limits.max_file_bytes
                                )
                            raise _AttachmentUnavailable("fetch_failed")
                        digest.update(chunk)
                        output.write(chunk)
            except urllib.error.HTTPError as exc:
                # A refused redirect lands here as its 3xx: fetch_failed.
                reason = {404: "not_found", 403: "forbidden"}.get(exc.code, "fetch_failed")
                raise _AttachmentUnavailable(reason) from exc
            except TimeoutError as exc:
                raise _AttachmentUnavailable("timeout") from exc
            except urllib.error.URLError as exc:
                timed_out = isinstance(exc.reason, TimeoutError)
                raise _AttachmentUnavailable("timeout" if timed_out else "fetch_failed") from exc
            except (OSError, http.client.HTTPException) as exc:
                raise _AttachmentUnavailable("fetch_failed") from exc
            if digest.hexdigest() != ref.sha256.lower():
                raise AttachmentResolutionError(
                    "attachments-fetch",
                    f"{ref.name!r} digest mismatch",
                )
        except BaseException:
            destination.unlink(missing_ok=True)
            raise
        # The runner runs as uid 1000 and the mount is read-only, so the bytes
        # only need to be readable; unlike the workspace they are never written
        # back.
        os.chmod(destination, 0o644)

    def _cleanup_attachments(self, name: str) -> None:
        tmp = self._attachment_dirs.pop(name, None)
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)
