"""Recovery checkpoint object transport, signed reads and collection (ADR 0251).

Recovery objects live under ``<prefix>/workspace-recovery/`` in the private
workspace bucket, a subtree disjoint from claim objects, so a claim release can
never delete them. The ledger (``WorkspaceRecoveryClient``) owns every key; this
module only moves bytes for keys the ledger minted. Logs and exceptions carry
metadata only: ids, kinds, sizes and typed codes, never bytes, URLs or signatures.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import shutil
import tempfile
import threading
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import httpx

from .workspace import PreparedWorkspace, WorkspaceClaimCoordinator, WorkspaceObjectStore
from .workspace_recovery import WorkspaceRecoveryClient

logger = logging.getLogger(__name__)

RECOVERY_NAMESPACE = "workspace-recovery"
CAPTURE_BUDGET_SECONDS = 60.0
CLEANUP_BUDGET_SECONDS = 5.0
STAGING_EXPIRY_SECONDS = 600
PATCH_LIMIT_BYTES = 900_000
MANIFEST_LIMIT_BYTES = 1_048_576
PART_BYTES = 8 * 1024 * 1024

_READ_CHUNK_BYTES = 1024 * 1024
_BASE_REQUIRED = "workspace_recovery.base_required"


def build_recovery_s3_client(
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    region: str,
    timeout_seconds: float = 10.0,
) -> Any:
    """A path-style S3 client with bounded timeouts and no hidden retries.

    ``total_max_attempts=1`` means one attempt in total (botocore's ``max_attempts``
    counts retries), so a storage fault surfaces inside the capture budget. SigV4
    signing makes every presigned read an ``X-Amz-Expires`` capability for one object.

    An empty credential becomes ``None`` so boto3 resolves the provider chain,
    as ``aci_protocol.s3.build_s3_client`` does.
    """

    import boto3
    from botocore.client import Config as BotoConfig

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key or None,
        aws_secret_access_key=secret_key or None,
        region_name=region,
        config=BotoConfig(
            connect_timeout=timeout_seconds,
            read_timeout=timeout_seconds,
            retries={"total_max_attempts": 1, "mode": "standard"},
            s3={"addressing_style": "path"},
            signature_version="s3v4",
        ),
    )


class RecoveryStorageError(RuntimeError):
    """A typed storage fault. The message is metadata only."""

    def __init__(self, code: str, message: str = "") -> None:
        self.code = code
        super().__init__(message or code)


def _error_code(exc: BaseException) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        error = response.get("Error")
        if isinstance(error, dict):
            return str(error.get("Code") or "")
    return ""


class RecoveryObjectStore(WorkspaceObjectStore):
    """Private recovery object storage, restricted to the recovery namespace."""

    def __init__(self, *, client: Any, bucket: str, prefix: str = "private") -> None:
        super().__init__(client=client, bucket=bucket, prefix=prefix)

    def _key(self, key: str) -> str:
        clean = key.lstrip("/")
        parts = clean.split("/")
        if ".." in parts or not (
            clean == RECOVERY_NAMESPACE or clean.startswith(f"{RECOVERY_NAMESPACE}/")
        ):
            raise ValueError("recovery object key is outside the recovery namespace")
        return f"{self._prefix}/{clean}"

    def upload_verified(
        self,
        key: str,
        source: Path,
        *,
        size_bytes: int,
        sha256: str,
        deadline: float,
        clock: Callable[[], float] = time.monotonic,
        cancelled: threading.Event | None = None,
    ) -> None:
        """Multipart-upload ``source`` to ``key``, then verify the stored bytes."""

        def check() -> None:
            if (cancelled is not None and cancelled.is_set()) or clock() >= deadline:
                raise RecoveryStorageError("capture_timeout")

        full = self._key(key)
        check()
        created = self._client.create_multipart_upload(Bucket=self._bucket, Key=full)
        upload_id = str(created["UploadId"])
        try:
            digest = hashlib.sha256()
            total = 0
            parts: list[dict[str, Any]] = []
            with source.open("rb") as handle:
                number = 1
                while True:
                    chunk = handle.read(PART_BYTES)
                    if not chunk and number > 1:
                        break
                    check()
                    digest.update(chunk)
                    total += len(chunk)
                    if total > size_bytes:
                        raise RecoveryStorageError("digest_mismatch")
                    response = self._client.upload_part(
                        Bucket=self._bucket,
                        Key=full,
                        UploadId=upload_id,
                        PartNumber=number,
                        Body=chunk,
                    )
                    parts.append({"ETag": response["ETag"], "PartNumber": number})
                    number += 1
                    if len(chunk) < PART_BYTES:
                        break
            if total != size_bytes or digest.hexdigest() != sha256:
                raise RecoveryStorageError("digest_mismatch")
            check()
            self._client.complete_multipart_upload(
                Bucket=self._bucket,
                Key=full,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
        except BaseException:
            try:
                self._client.abort_multipart_upload(
                    Bucket=self._bucket, Key=full, UploadId=upload_id
                )
            except Exception:  # noqa: BLE001 - typed fault boundary
                logger.debug("recovery multipart abort failed; the collector retries it")
            raise
        self.verify(key, size_bytes=size_bytes, sha256=sha256)

    def verify(self, key: str, *, size_bytes: int, sha256: str) -> None:
        """Check the stored length by HEAD and the digest by a full read-back."""

        head = self._client.head_object(Bucket=self._bucket, Key=self._key(key))
        if int(head.get("ContentLength", -1)) != size_bytes:
            raise RecoveryStorageError("digest_mismatch")
        digest = hashlib.sha256()
        total = 0
        for chunk in self.get_stream(key):
            digest.update(chunk)
            total += len(chunk)
        if total != size_bytes or digest.hexdigest() != sha256:
            raise RecoveryStorageError("digest_mismatch")

    def pending_uploads(self, key: str) -> list[str]:
        """Upload ids of multipart uploads pending for exactly ``key``."""

        full = self._key(key)
        paginator = self._client.get_paginator("list_multipart_uploads")
        upload_ids: list[str] = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=full):
            for upload in page.get("Uploads") or ():
                if upload.get("Key") == full and upload.get("UploadId"):
                    upload_ids.append(str(upload["UploadId"]))
        return upload_ids

    def abort_uploads(self, key: str) -> int:
        """Abort every pending multipart upload for ``key``; returns the count."""

        full = self._key(key)
        aborted = 0
        for upload_id in self.pending_uploads(key):
            try:
                self._client.abort_multipart_upload(
                    Bucket=self._bucket, Key=full, UploadId=upload_id
                )
            except Exception as exc:
                if _error_code(exc) != "NoSuchUpload":
                    raise
                continue
            aborted += 1
        return aborted

    def discard(self, key: str) -> None:
        """Abort pending uploads and delete the object. A missing object is fine."""

        self.abort_uploads(key)
        self.delete(key)


@dataclass(frozen=True)
class RecoveryAuthority:
    work_item_id: uuid.UUID
    source_request_id: uuid.UUID
    runtime_epoch: int
    dispatch_generation: int

    def payload(self) -> dict[str, Any]:
        return {
            "work_item_id": str(self.work_item_id),
            "source_request_id": str(self.source_request_id),
            "runtime_epoch": self.runtime_epoch,
            "dispatch_generation": self.dispatch_generation,
        }


@dataclass(frozen=True)
class RecoveryRepository:
    provider: str
    host: str
    project_id: str
    path: str


@dataclass(frozen=True)
class RecoveryBaseSource:
    """The claim's trusted base archive: its digest and a byte stream."""

    sha256: str
    open: Callable[[], Iterable[bytes]]

    @classmethod
    def from_prepared(
        cls, coordinator: WorkspaceClaimCoordinator, prepared: PreparedWorkspace
    ) -> RecoveryBaseSource:
        """The base of an already-read claim. Touches no store until ``open``.

        The writer calls ``open`` inside its spool thread, so building this on the
        event loop never blocks it.
        """

        objects = coordinator.preparer.objects
        object_key = prepared.object_key
        return cls(sha256=prepared.sha256, open=lambda: objects.get_stream(object_key))

    @classmethod
    def from_claim(
        cls, coordinator: WorkspaceClaimCoordinator, thread_key: str
    ) -> RecoveryBaseSource | None:
        """Read the thread's claim, then its base. Synchronous: call off the loop."""

        prepared = coordinator.current(thread_key)
        if prepared is None:
            return None
        return cls.from_prepared(coordinator, prepared)


@dataclass(frozen=True)
class RecoveryCaptureRequest:
    authority: RecoveryAuthority
    idempotency_key: str
    repository: RecoveryRepository
    base_commit: str
    target_branch: str
    patch: bytes
    manifest: bytes
    base: RecoveryBaseSource | None


FailureCode = Literal[
    "patch_too_large",
    "manifest_too_large",
    "base_too_large",
    "base_unavailable",
    "base_digest_mismatch",
    "reserve_refused",
    "ledger_unavailable",
    "upload_failed",
    "digest_mismatch",
    "capture_timeout",
    "complete_failed",
    "lease_lost",
]


@dataclass(frozen=True)
class RecoveryCaptureResult:
    status: Literal["complete", "failed"]
    checkpoint_id: uuid.UUID | None
    failure: FailureCode | None = None
    refusal: str | None = None
    cleanup_complete: bool = True


class _CaptureFailure(Exception):
    def __init__(
        self, code: FailureCode, refusal: str | None = None, *, retract: bool = False
    ) -> None:
        self.code: FailureCode = code
        self.refusal = refusal
        # Set only when this invocation created its reservation (not a replay) and
        # has sent a ``complete``: the one case allowed to withdraw a checkpoint.
        self.retract = retract
        super().__init__(code)


class _LedgerRefused(Exception):
    def __init__(self, status: int, code: str | None) -> None:
        self.status = status
        self.code = code
        super().__init__(code or f"http_{status}")


class _LedgerUnavailable(Exception):
    pass


def _refusal_code(exc: httpx.HTTPStatusError) -> str | None:
    try:
        body = exc.response.json()
    except Exception:  # noqa: BLE001 - typed fault boundary
        return None
    detail = body.get("detail") if isinstance(body, dict) else None
    code = detail.get("code") if isinstance(detail, dict) else None
    return code if isinstance(code, str) else None


async def _ledger_call(
    call: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
    payload: dict[str, Any],
    timeout: float | None = None,
) -> dict[str, Any]:
    """Run one ledger call, translating faults into typed exceptions.

    Raises ``_LedgerRefused`` for a 4xx and ``_LedgerUnavailable`` for a 5xx,
    a transport fault or malformed metadata; ``TimeoutError`` when ``timeout``
    elapses.
    """

    try:
        return await asyncio.wait_for(call(payload), timeout)
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if 400 <= status < 500:
            raise _LedgerRefused(status, _refusal_code(exc)) from None
        raise _LedgerUnavailable() from None
    except (httpx.HTTPError, ValueError):
        raise _LedgerUnavailable() from None


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_private(path: Path, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)


_SIZE_REFUSALS: dict[str, FailureCode] = {
    "workspace_recovery.patch_too_large": "patch_too_large",
    "workspace_recovery.manifest_too_large": "manifest_too_large",
    "workspace_recovery.base_too_large": "base_too_large",
}

_FAIL_REASON: dict[str, str] = {
    "capture_timeout": "capture_timeout",
    "digest_mismatch": "capture_failed",
    "lease_lost": "lease_lost",
}

# Awaited before a completion is attempted and once more after it commits; any
# exception means the caller's exclusive ownership of the workspace is gone.
Fence = Callable[[], Awaitable[None]]


async def _fenced(fence: Fence | None) -> None:
    if fence is None:
        return
    try:
        await fence()
    except Exception:  # noqa: BLE001 - any fence fault is a lost lease
        raise _CaptureFailure("lease_lost") from None


class RecoveryCheckpointWriter:
    """Reserve, upload, verify and complete one recovery checkpoint."""

    def __init__(
        self,
        *,
        ledger: WorkspaceRecoveryClient,
        store: RecoveryObjectStore,
        scratch_root: Path,
        base_archive_limit_bytes: int,
        capture_budget_seconds: float = CAPTURE_BUDGET_SECONDS,
        cleanup_budget_seconds: float = CLEANUP_BUDGET_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ledger = ledger
        self._store = store
        self._scratch_root = scratch_root
        self._base_limit = base_archive_limit_bytes
        self._capture_budget = capture_budget_seconds
        self._cleanup_budget = cleanup_budget_seconds
        self._clock = clock

    async def capture(
        self,
        request: RecoveryCaptureRequest,
        *,
        deadline: float | None = None,
        fence: Fence | None = None,
    ) -> RecoveryCaptureResult:
        """Capture one checkpoint. Storage, ledger and network faults never raise.

        ``deadline`` is absolute on this writer's clock and replaces the per-call
        budget, so one aggregate deadline can cover work done before this call.
        ``fence`` is awaited before every completion attempt and once after the
        completion commits. A fence failing before completion tombstones the
        staging; failing after this call sent its own completion retracts the
        checkpoint if it committed, whose objects are then left to the collector. A
        replay of an already-complete checkpoint never asks for a retraction.
        """

        if deadline is None:
            deadline = self._clock() + self._capture_budget
        # Size caps are the ledger's: it records the refusal as the WorkItem's latest
        # recovery outcome (ADR 0251 section 3), so there is no local precheck.
        sizes: dict[str, int] = {"patch": len(request.patch), "manifest": len(request.manifest)}
        cancel = threading.Event()
        workdir: Path | None = None
        reservation: dict[str, Any] | None = None
        declarations: dict[str, dict[str, Any]] = {
            "patch": {
                "kind": "patch",
                "size_bytes": sizes["patch"],
                "sha256": _sha256(request.patch),
            },
            "manifest": {
                "kind": "manifest",
                "size_bytes": sizes["manifest"],
                "sha256": _sha256(request.manifest),
            },
        }
        # Set once any ``complete`` request has been sent: the writer cannot tell
        # whether a sent request committed, so every later failure may withdraw it.
        sent: list[bool] = []

        def may_retract() -> bool:
            # Only a reservation this invocation created may be withdrawn: a replay's
            # ``complete`` answers success for a checkpoint an earlier invocation
            # completed, and proves nothing about who did.
            return bool(sent) and reservation is not None and reservation["replayed"] is False

        try:
            self._scratch_root.mkdir(parents=True, exist_ok=True)
            workdir = Path(tempfile.mkdtemp(prefix="recovery-", dir=self._scratch_root))
            sources = {"patch": workdir / "patch", "manifest": workdir / "manifest"}
            for kind, data in (("patch", request.patch), ("manifest", request.manifest)):
                remaining = deadline - self._clock()
                if remaining <= 0:
                    raise _CaptureFailure("capture_timeout")
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(_write_private, sources[kind], data), remaining
                    )
                except TimeoutError:
                    raise _CaptureFailure("capture_timeout") from None
            try:
                reservation = await self._reserve(request, declarations, deadline)
            except _CaptureFailure as failure:
                if failure.refusal != _BASE_REQUIRED:
                    raise
                if request.base is None:
                    raise _CaptureFailure("base_unavailable") from None
                sources["base"] = workdir / "base"
                size, digest, oversized = await self._spool_base(
                    request.base, sources["base"], deadline, cancel
                )
                sizes["base"] = size
                declarations["base"] = {"kind": "base", "size_bytes": size, "sha256": digest}
                # An oversized base is declared at its true size so the ledger
                # refuses it with ``base_too_large`` and records that outcome.
                reservation = await self._reserve(request, declarations, deadline)
                if oversized:
                    raise _CaptureFailure("base_too_large") from None
            await self._upload(reservation, declarations, sources, deadline, cancel)
            await self._complete(request, reservation, deadline, fence, sent)
            await _fenced(fence)
            result = RecoveryCaptureResult("complete", _checkpoint_id(reservation))
        except _CaptureFailure as failure:
            cancel.set()
            failure.retract = may_retract()
            result = await self._failed(request, reservation, set(declarations), failure)
        except Exception:  # noqa: BLE001 - typed fault boundary
            cancel.set()
            code: FailureCode = "ledger_unavailable" if reservation is None else "upload_failed"
            result = await self._failed(
                request,
                reservation,
                set(declarations),
                _CaptureFailure(code, retract=may_retract()),
            )
        finally:
            cancel.set()
            if workdir is not None:
                shutil.rmtree(workdir, ignore_errors=True)
        return self._outcome(request, result, sizes)

    async def _reserve(
        self,
        request: RecoveryCaptureRequest,
        declarations: dict[str, dict[str, Any]],
        deadline: float,
    ) -> dict[str, Any]:
        remaining = deadline - self._clock()
        if remaining <= 0:
            raise _CaptureFailure("capture_timeout")
        payload = {
            **request.authority.payload(),
            "idempotency_key": request.idempotency_key,
            "repository": asdict(request.repository),
            "base_commit": request.base_commit,
            "target_branch": request.target_branch,
            "base_archive_limit_bytes": self._base_limit,
            "objects": list(declarations.values()),
            # The ledger's capture deadline is its ``now`` plus what is left of
            # this capture's aggregate budget, never a fresh full budget.
            "capture_budget_seconds": min(CAPTURE_BUDGET_SECONDS, remaining),
        }
        try:
            reservation = await _ledger_call(self._ledger.reserve, payload, remaining)
        except TimeoutError:
            raise _CaptureFailure("capture_timeout") from None
        except _LedgerRefused as refused:
            code = _SIZE_REFUSALS.get(refused.code or "", "reserve_refused")
            raise _CaptureFailure(code, refused.code) from None
        except _LedgerUnavailable:
            raise _CaptureFailure("ledger_unavailable") from None
        try:
            _checkpoint_id(reservation)
            objects = reservation["objects"]
            if not isinstance(objects, list) or not isinstance(reservation["replayed"], bool):
                raise TypeError
        except (KeyError, TypeError, ValueError):
            raise _CaptureFailure("ledger_unavailable") from None
        return reservation

    async def _spool_base(
        self,
        base: RecoveryBaseSource,
        destination: Path,
        deadline: float,
        cancel: threading.Event,
    ) -> tuple[int, str, bool]:
        """Spool the base privately while hashing; returns size, digest and oversize.

        Past the archive limit the spool stops writing but keeps hashing, so the
        true size and digest can be declared and the ledger records the refusal.
        """

        limit = self._base_limit
        clock = self._clock

        def spool() -> tuple[int, str, bool]:
            digest = hashlib.sha256()
            total = 0
            fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            chunks = base.open()
            try:
                with os.fdopen(fd, "wb") as handle:
                    for chunk in chunks:
                        if cancel.is_set() or clock() >= deadline:
                            raise _CaptureFailure("capture_timeout")
                        total += len(chunk)
                        digest.update(chunk)
                        if total <= limit:
                            handle.write(chunk)
            finally:
                close = getattr(chunks, "close", None)
                if close is not None:
                    close()
            return total, digest.hexdigest(), total > limit

        remaining = deadline - clock()
        if remaining <= 0:
            raise _CaptureFailure("capture_timeout")
        try:
            outcome = await asyncio.wait_for(asyncio.to_thread(spool), remaining)
        except TimeoutError:
            cancel.set()
            raise _CaptureFailure("capture_timeout") from None
        except _CaptureFailure:
            raise
        except Exception:  # noqa: BLE001 - typed fault boundary
            raise _CaptureFailure("base_unavailable") from None
        size, digest, oversized = outcome
        if not oversized and digest != base.sha256:
            raise _CaptureFailure("base_digest_mismatch")
        return size, digest, oversized

    async def _upload(
        self,
        reservation: dict[str, Any],
        declarations: dict[str, dict[str, Any]],
        sources: dict[str, Path],
        deadline: float,
        cancel: threading.Event,
    ) -> None:
        for obj in reservation["objects"]:
            kind = str(obj["kind"])
            if kind not in declarations:
                continue
            remaining = deadline - self._clock()
            if remaining <= 0:
                raise _CaptureFailure("capture_timeout")
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(
                        self._store.upload_verified,
                        str(obj["key"]),
                        sources[kind],
                        size_bytes=int(obj["size_bytes"]),
                        sha256=str(obj["sha256"]),
                        deadline=deadline,
                        clock=self._clock,
                        cancelled=cancel,
                    ),
                    remaining,
                )
            except TimeoutError:
                cancel.set()
                raise _CaptureFailure("capture_timeout") from None
            except RecoveryStorageError as exc:
                code: FailureCode = "upload_failed"
                if exc.code == "digest_mismatch":
                    code = "digest_mismatch"
                elif exc.code == "capture_timeout":
                    code = "capture_timeout"
                raise _CaptureFailure(code) from None
            except Exception:  # noqa: BLE001 - typed fault boundary
                raise _CaptureFailure("upload_failed") from None

    async def _complete(
        self,
        request: RecoveryCaptureRequest,
        reservation: dict[str, Any],
        deadline: float,
        fence: Fence | None,
        sent: list[bool],
    ) -> None:
        payload = {
            **request.authority.payload(),
            "checkpoint_id": str(_checkpoint_id(reservation)),
            "receipts": reservation["objects"],
        }
        for _attempt in range(2):
            remaining = deadline - self._clock()
            if remaining <= 0:
                break
            await _fenced(fence)
            remaining = deadline - self._clock()
            if remaining <= 0:
                break
            try:
                sent.append(True)
                await _ledger_call(self._ledger.complete, payload, remaining)
                return
            except _LedgerRefused as refused:
                raise _CaptureFailure("complete_failed", refused.code) from None
            except TimeoutError:
                break
            except _LedgerUnavailable:
                continue
        raise _CaptureFailure("complete_failed")

    async def _failed(
        self,
        request: RecoveryCaptureRequest,
        reservation: dict[str, Any] | None,
        declared: set[str],
        failure: _CaptureFailure,
    ) -> RecoveryCaptureResult:
        if reservation is None:
            return RecoveryCaptureResult("failed", None, failure.code, failure.refusal)
        checkpoint_id = _checkpoint_id(reservation)
        try:
            cleanup_complete = await asyncio.wait_for(
                self._cleanup(request, reservation, declared, failure),
                self._cleanup_budget,
            )
        except Exception:  # noqa: BLE001 - typed fault boundary
            cleanup_complete = False
        return RecoveryCaptureResult(
            "failed", checkpoint_id, failure.code, failure.refusal, cleanup_complete
        )

    async def _cleanup(
        self,
        request: RecoveryCaptureRequest,
        reservation: dict[str, Any],
        declared: set[str],
        failure: _CaptureFailure,
    ) -> bool:
        """Tombstone the reservation, then discard exactly the keys this capture declared.

        Only a failure carrying ``retract`` (this call's own completion returned,
        then its fence failed) asks the ledger to withdraw a complete checkpoint.
        A ``retracted`` answer, including a repeated fail on a checkpoint that was
        once complete, discards nothing.

        Objects are discarded only after ``fail`` confirms the tombstone. A replayed
        reservation can name a checkpoint that is already complete (or whose
        ``complete`` committed with its response lost); ``fail`` refuses it, and
        deleting its keys then would destroy a good checkpoint. When ``fail`` is
        refused or unavailable nothing is discarded: the ledger's staging rows are
        the durable record the collector acts on, and cleanup reports incomplete.
        """

        keys = [str(obj["key"]) for obj in reservation["objects"] if str(obj["kind"]) in declared]
        payload = {
            **request.authority.payload(),
            "checkpoint_id": str(_checkpoint_id(reservation)),
            "reason": _FAIL_REASON.get(failure.code, "upload_failed"),
            "retract": failure.retract,
        }
        try:
            response = await _ledger_call(self._ledger.fail, payload)
        except Exception:  # noqa: BLE001 - typed fault boundary
            return False
        status = response.get("status")
        if status == "retracted":
            # A completed checkpoint withdrawn after a lost lease: a reader may
            # already hold it, so its objects are the collector's to remove.
            return True
        if status != "tombstoned":
            return False
        results = await asyncio.gather(
            *(asyncio.to_thread(self._store.discard, key) for key in keys),
            return_exceptions=True,
        )
        return not any(isinstance(item, BaseException) for item in results)

    def _outcome(
        self,
        request: RecoveryCaptureRequest,
        result: RecoveryCaptureResult,
        sizes: dict[str, int] | None = None,
    ) -> RecoveryCaptureResult:
        sizes = sizes or {}
        logger.info(
            "workspace recovery capture status=%s work_item_id=%s checkpoint_id=%s "
            "kinds=%s sizes=%s failure=%s refusal=%s cleanup_complete=%s",
            result.status,
            request.authority.work_item_id,
            result.checkpoint_id,
            ",".join(sorted(sizes)),
            ",".join(f"{kind}:{sizes[kind]}" for kind in sorted(sizes)),
            result.failure,
            result.refusal,
            result.cleanup_complete,
        )
        return result


def _checkpoint_id(reservation: dict[str, Any]) -> uuid.UUID:
    return uuid.UUID(str(reservation["checkpoint_id"]))


@dataclass(frozen=True)
class RecoveryObjectRef:
    kind: str
    key: str
    size_bytes: int
    sha256: str


@dataclass
class RecoveryReadLease:
    lease_id: uuid.UUID
    checkpoint_id: uuid.UUID
    work_item_id: uuid.UUID
    holder_id: str
    expires_at: datetime
    objects: dict[str, RecoveryObjectRef]
    released: bool = False


class RecoveryReadRefused(Exception):
    """A refused or failed recovery read. The message is the code, never a URL."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _aware_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


class RecoveryCheckpointReader:
    """Reader leases plus signed, verified reads of one checkpoint's objects."""

    def __init__(
        self,
        *,
        ledger: WorkspaceRecoveryClient,
        store: RecoveryObjectStore,
        transport: httpx.AsyncBaseTransport,
        reference_ttl_seconds: int,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        # Signed GETs go straight to a transport, never an ``httpx`` client, whose
        # logger records full request URLs (and so the signature) at INFO.
        self._ledger = ledger
        self._store = store
        self._transport = transport
        self._ttl = reference_ttl_seconds
        self._wall_clock = wall_clock

    async def acquire(
        self, *, work_item_id: uuid.UUID, checkpoint_id: uuid.UUID, holder_id: str
    ) -> RecoveryReadLease:
        payload = {
            "work_item_id": str(work_item_id),
            "checkpoint_id": str(checkpoint_id),
            "holder_id": holder_id,
        }
        try:
            raw = await _ledger_call(self._ledger.acquire_reader, payload)
        except _LedgerRefused as refused:
            raise RecoveryReadRefused(refused.code or f"http_{refused.status}") from None
        except _LedgerUnavailable:
            raise RecoveryReadRefused("ledger_unavailable") from None
        try:
            objects = {
                str(item["kind"]): RecoveryObjectRef(
                    kind=str(item["kind"]),
                    key=str(item["key"]),
                    size_bytes=int(item["size_bytes"]),
                    sha256=str(item["sha256"]),
                )
                for item in raw["objects"]
            }
            lease = RecoveryReadLease(
                lease_id=uuid.UUID(str(raw["lease_id"])),
                checkpoint_id=uuid.UUID(str(raw["checkpoint_id"])),
                work_item_id=work_item_id,
                holder_id=str(raw["holder_id"]),
                expires_at=_aware_utc(str(raw["expires_at"])),
                objects=objects,
            )
        except (KeyError, TypeError, ValueError):
            raise RecoveryReadRefused("ledger_unavailable") from None
        logger.info(
            "workspace recovery reader acquired work_item_id=%s checkpoint_id=%s lease_id=%s",
            work_item_id,
            lease.checkpoint_id,
            lease.lease_id,
        )
        return lease

    def capability(self, lease: RecoveryReadLease, kind: str) -> str:
        """A signed GET for one object, expiring no later than the lease."""

        if lease.released:
            raise RecoveryReadRefused("lease_released")
        remaining = math.floor(lease.expires_at.timestamp() - self._wall_clock())
        if remaining < 1:
            raise RecoveryReadRefused("lease_expired")
        ref = lease.objects.get(kind)
        if ref is None:
            raise RecoveryReadRefused("unknown_object")
        return self._store.presign_get(ref.key, expires_seconds=min(self._ttl, remaining))

    async def read(self, lease: RecoveryReadLease, kind: str, destination: Path) -> None:
        """Stream one object to ``destination`` (mode 0600) and verify it."""

        url = self.capability(lease, kind)
        ref = lease.objects[kind]
        # The whole read (request, headers, body, writes, verification) must finish
        # inside the lease that authorized it (ADR 0251 section 4).
        budget = lease.expires_at.timestamp() - self._wall_clock()
        if budget <= 0:
            raise RecoveryReadRefused("lease_expired")
        request = httpx.Request(
            "GET",
            url,
            extensions={
                "timeout": {"connect": budget, "read": budget, "write": budget, "pool": budget}
            },
        )
        fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        os.fchmod(fd, 0o600)

        async def stream() -> str | None:
            with os.fdopen(fd, "wb") as handle:
                digest = hashlib.sha256()
                total = 0
                response = await self._transport.handle_async_request(request)
                try:
                    if response.status_code != 200:
                        return "read_refused"
                    async for chunk in response.aiter_raw(_READ_CHUNK_BYTES):
                        total += len(chunk)
                        if total > ref.size_bytes:
                            return "digest_mismatch"
                        digest.update(chunk)
                        handle.write(chunk)
                finally:
                    await response.aclose()
            if total != ref.size_bytes or digest.hexdigest() != ref.sha256:
                return "digest_mismatch"
            return None

        try:
            failure = await asyncio.wait_for(stream(), budget)
        except TimeoutError:
            failure = "lease_expired"
        except Exception:  # noqa: BLE001 - typed fault boundary
            failure = "read_failed"
        if failure is not None:
            destination.unlink(missing_ok=True)
            logger.info(
                "workspace recovery read refused lease_id=%s kind=%s code=%s",
                lease.lease_id,
                kind,
                failure,
            )
            raise RecoveryReadRefused(failure)

    async def release(self, lease: RecoveryReadLease) -> None:
        try:
            await _ledger_call(
                self._ledger.release_reader,
                {"lease_id": str(lease.lease_id), "holder_id": lease.holder_id},
            )
        except Exception as exc:  # noqa: BLE001 - typed fault boundary
            logger.warning(
                "workspace recovery reader release failed lease_id=%s cause=%s",
                lease.lease_id,
                type(exc).__name__,
            )
        finally:
            lease.released = True


@dataclass(frozen=True)
class RecoveryCollectionPass:
    claimed: int
    deleted: int
    finalized: int
    conflicts: int
    failed: int


class RecoveryCollector:
    """Delete ledger-claimed recovery objects outside any lock, then finalize them."""

    def __init__(
        self,
        *,
        ledger: WorkspaceRecoveryClient,
        store: RecoveryObjectStore,
        collector_id: str,
        limit: int = 100,
    ) -> None:
        self._ledger = ledger
        self._store = store
        self._collector_id = collector_id
        self._limit = limit

    async def collect_once(self) -> RecoveryCollectionPass:
        """One pass. Raises ``RecoveryStorageError("ledger_unavailable")`` if claiming fails."""

        try:
            raw = await _ledger_call(
                self._ledger.claim_collection,
                {"collector_id": self._collector_id, "limit": self._limit},
            )
            claims = list(raw["claims"])
        except (_LedgerRefused, _LedgerUnavailable, KeyError, TypeError):
            raise RecoveryStorageError("ledger_unavailable") from None
        deleted = finalized = conflicts = failed = 0
        for claim in claims:
            try:
                key = str(claim["key"])
                payload = {
                    "collector_id": self._collector_id,
                    "object_id": str(claim["object_id"]),
                    "claim_id": str(claim["claim_id"]),
                    "version": int(claim["version"]),
                }
                await asyncio.to_thread(self._store.discard, key)
            except Exception:  # noqa: BLE001 - typed fault boundary
                failed += 1
                continue
            deleted += 1
            try:
                await _ledger_call(self._ledger.finalize_collection, payload)
            except _LedgerRefused as refused:
                if refused.status == 409:
                    conflicts += 1
                else:
                    failed += 1
                continue
            except Exception:  # noqa: BLE001 - typed fault boundary
                failed += 1
                continue
            finalized += 1
        result = RecoveryCollectionPass(len(claims), deleted, finalized, conflicts, failed)
        logger.log(
            logging.INFO if claims else logging.DEBUG,
            "workspace recovery collection claimed=%d deleted=%d finalized=%d "
            "conflicts=%d failed=%d",
            result.claimed,
            result.deleted,
            result.finalized,
            result.conflicts,
            result.failed,
        )
        return result

    async def run_forever(self, shutdown: asyncio.Event, *, interval_s: float = 30.0) -> None:
        while not shutdown.is_set():
            try:
                await self.collect_once()
            except RecoveryStorageError as exc:
                logger.warning("workspace recovery collection skipped code=%s", exc.code)
            except Exception as exc:  # noqa: BLE001 - typed fault boundary
                logger.warning(
                    "workspace recovery collection pass failed cause=%s", type(exc).__name__
                )
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=interval_s)
            except TimeoutError:
                pass
