"""Publication candidate object transport, signed reads and collection (#5428).

Candidate objects live under ``<prefix>/publication-candidates/`` in the private
workspace bucket, a subtree disjoint from claim objects and from recovery
checkpoints, so neither a claim release nor recovery collection can delete them.
The ledger (``PublicationCandidateClient``) owns every key; this module only
moves bytes for keys the ledger minted. Logs and exceptions carry metadata only:
ids, kinds, sizes and typed codes, never bytes, URLs, signatures, titles, bodies
or paths.

A candidate grants nothing. It retains the exact snapshot a publication request
produced and the trusted base it applies to, so a separate consumer can recover
the work; reading one never transfers ownership or renews retention.
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
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar, Literal

import httpx

from .publication_candidates import PublicationCandidateClient
from .workspace_recovery_storage import (
    RecoveryBaseSource,
    RecoveryCollector,
    RecoveryObjectStore,
    RecoveryRepository,
    RecoveryStorageError,
)

logger = logging.getLogger(__name__)

CANDIDATE_NAMESPACE = "publication-candidates"
CAPTURE_BUDGET_SECONDS = 60.0
CLEANUP_BUDGET_SECONDS = 5.0
PATCH_LIMIT_BYTES = 900_000
MAX_CHANGED_PATHS = 4096
MAX_CHANGED_PATH_CHARS = 4096
COLLECTOR_LABEL = "publication candidate"

_READ_CHUNK_BYTES = 1024 * 1024
_CODE_PREFIX = "publication_candidates."
_BASE_REQUIRED = f"{_CODE_PREFIX}base_required"
_DISABLED = f"{_CODE_PREFIX}disabled"

# The repository identity a candidate binds; the same four facts as recovery.
CandidateRepository = RecoveryRepository


class CandidateObjectStore(RecoveryObjectStore):
    """Private candidate object storage, restricted to the candidate namespace."""

    namespace: ClassVar[str] = CANDIDATE_NAMESPACE
    _outside_message: ClassVar[str] = (
        "publication candidate object key is outside the candidate namespace"
    )


def build_candidate_collector(
    *, ledger: PublicationCandidateClient, store: CandidateObjectStore, collector_id: str
) -> RecoveryCollector:
    """The shared claim, delete and finalize collector over the candidate ledger."""

    return RecoveryCollector(
        ledger=ledger, store=store, collector_id=collector_id, label=COLLECTOR_LABEL
    )


@dataclass(frozen=True)
class CandidateAuthority:
    work_item_id: uuid.UUID
    request_id: uuid.UUID
    runtime_epoch: int
    dispatch_generation: int

    def payload(self) -> dict[str, Any]:
        return {
            "work_item_id": str(self.work_item_id),
            "request_id": str(self.request_id),
            "runtime_epoch": self.runtime_epoch,
            "dispatch_generation": self.dispatch_generation,
        }


@dataclass(frozen=True)
class CandidateCaptureRequest:
    authority: CandidateAuthority
    dedupe_key: str
    repository: CandidateRepository
    base_commit: str
    target_branch: str
    patch: bytes
    changed_paths: tuple[str, ...]
    contains_workflow_files: bool
    title: str
    body: str
    base: RecoveryBaseSource | None


CandidateFailure = Literal[
    "base_too_large",
    "base_unavailable",
    "base_digest_mismatch",
    "reserve_refused",
    "ledger_unavailable",
    "ledger_disabled",
    "upload_failed",
    "digest_mismatch",
    "capture_timeout",
    "complete_failed",
]


@dataclass(frozen=True)
class CandidateWriteResult:
    """What one write did.

    ``failure`` is the writer's own code; ``refusal`` is the ledger's full code
    when a ledger answer ended the write (a recorded refusal such as
    ``publication_candidates.installation_capacity`` included), else None.
    ``candidate_id`` is None exactly when no reservation was obtained.
    """

    status: Literal["complete", "failed"]
    candidate_id: uuid.UUID | None
    failure: CandidateFailure | None = None
    refusal: str | None = None
    cleanup_complete: bool = True
    replayed: bool | None = None


class _CaptureFailure(Exception):
    def __init__(self, code: CandidateFailure, refusal: str | None = None) -> None:
        self.code: CandidateFailure = code
        self.refusal = refusal
        super().__init__(code)


class _LedgerRefused(Exception):
    def __init__(self, status: int, code: str | None) -> None:
        self.status = status
        self.code = code
        super().__init__(code or f"http_{status}")


class _LedgerUnavailable(Exception):
    pass


class _LedgerDisabled(Exception):
    pass


def refusal_code(response: httpx.Response) -> str | None:
    """The ledger's ``detail.code``, or None for any other body."""

    try:
        body = response.json()
    except Exception:  # noqa: BLE001 - typed fault boundary
        return None
    detail = body.get("detail") if isinstance(body, dict) else None
    code = detail.get("code") if isinstance(detail, dict) else None
    return code if isinstance(code, str) else None


def short_code(code: str) -> str:
    """``publication_candidates.installation_capacity`` -> ``installation_capacity``."""

    return code.removeprefix(_CODE_PREFIX)


async def _ledger_call(
    call: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
    payload: dict[str, Any],
    timeout: float | None = None,
) -> dict[str, Any]:
    """Run one ledger call, translating faults into typed exceptions.

    Raises ``_LedgerRefused`` for a 4xx, ``_LedgerDisabled`` for the ledger's
    coded 503 ``disabled`` answer, ``_LedgerUnavailable`` for any other 5xx, a
    transport fault or malformed metadata, and ``TimeoutError`` when ``timeout``
    elapses.
    """

    try:
        return await asyncio.wait_for(call(payload), timeout)
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if 400 <= status < 500:
            raise _LedgerRefused(status, refusal_code(exc.response)) from None
        if status == 503 and refusal_code(exc.response) == _DISABLED:
            raise _LedgerDisabled() from None
        raise _LedgerUnavailable() from None
    except (httpx.HTTPError, ValueError):
        raise _LedgerUnavailable() from None


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_private(path: Path, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)


# The writer's failure and the ``fail`` reason the ledger accepts for it.
_FAIL_REASON: dict[str, str] = {
    "capture_timeout": "capture_timeout",
    "digest_mismatch": "capture_failed",
    "complete_failed": "capture_failed",
}


def _candidate_id(reservation: dict[str, Any]) -> uuid.UUID:
    return uuid.UUID(str(reservation["candidate_id"]))


class CandidateWriter:
    """Reserve, upload, verify and complete one publication candidate."""

    def __init__(
        self,
        *,
        ledger: PublicationCandidateClient,
        store: CandidateObjectStore,
        scratch_root: Path,
        base_archive_limit_bytes: int,
        patch_limit_bytes: int = PATCH_LIMIT_BYTES,
        capture_budget_seconds: float = CAPTURE_BUDGET_SECONDS,
        cleanup_budget_seconds: float = CLEANUP_BUDGET_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not 0 < patch_limit_bytes <= PATCH_LIMIT_BYTES:
            raise ValueError("candidate patch limit must be in [1, 900000] bytes")
        self._ledger = ledger
        self._store = store
        self._scratch_root = scratch_root
        self._base_limit = base_archive_limit_bytes
        self._patch_limit = patch_limit_bytes
        self._capture_budget = capture_budget_seconds
        self._cleanup_budget = cleanup_budget_seconds
        self._clock = clock

    async def capture(
        self, request: CandidateCaptureRequest, *, deadline: float | None = None
    ) -> CandidateWriteResult:
        """Write one candidate. Storage, ledger and network faults never raise.

        ``deadline`` is absolute on this writer's clock and replaces the per-call
        budget, so one aggregate deadline can cover work done before this call.
        The first reservation declares no base: a WorkItem retains one base for all
        its candidates, and only when the ledger answers ``base_required`` is the
        claim base spooled and declared. Size and capacity caps are the ledger's,
        which records each refusal as the WorkItem's visible outcome.
        """

        if deadline is None:
            deadline = self._clock() + self._capture_budget
        sizes: dict[str, int] = {"patch": len(request.patch)}
        cancel = threading.Event()
        workdir: Path | None = None
        reservation: dict[str, Any] | None = None
        declarations: dict[str, dict[str, Any]] = {
            "patch": {
                "kind": "patch",
                "size_bytes": sizes["patch"],
                "sha256": _sha256(request.patch),
            }
        }
        try:
            self._scratch_root.mkdir(parents=True, exist_ok=True)
            workdir = Path(tempfile.mkdtemp(prefix="candidate-", dir=self._scratch_root))
            sources = {"patch": workdir / "patch"}
            remaining = deadline - self._clock()
            if remaining <= 0:
                raise _CaptureFailure("capture_timeout")
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(_write_private, sources["patch"], request.patch), remaining
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
            await self._complete(request, reservation, deadline)
            result = CandidateWriteResult(
                "complete", _candidate_id(reservation), replayed=bool(reservation["replayed"])
            )
        except _CaptureFailure as failure:
            cancel.set()
            result = await self._failed(request, reservation, set(declarations), failure)
        except Exception:  # noqa: BLE001 - typed fault boundary
            cancel.set()
            code: CandidateFailure = (
                "ledger_unavailable" if reservation is None else "upload_failed"
            )
            result = await self._failed(
                request, reservation, set(declarations), _CaptureFailure(code)
            )
        finally:
            cancel.set()
            if workdir is not None:
                shutil.rmtree(workdir, ignore_errors=True)
        return self._outcome(request, result, sizes)

    async def _reserve(
        self,
        request: CandidateCaptureRequest,
        declarations: dict[str, dict[str, Any]],
        deadline: float,
    ) -> dict[str, Any]:
        remaining = deadline - self._clock()
        if remaining <= 0:
            raise _CaptureFailure("capture_timeout")
        payload = {
            **request.authority.payload(),
            "dedupe_key": request.dedupe_key,
            "repository": asdict(request.repository),
            "base_commit": request.base_commit,
            "target_branch": request.target_branch,
            "base_archive_limit_bytes": self._base_limit,
            "patch_limit_bytes": self._patch_limit,
            "changed_paths": list(request.changed_paths),
            "contains_workflow_files": request.contains_workflow_files,
            "title": request.title,
            "body": request.body,
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
            code = refused.code or f"http_{refused.status}"
            raise _CaptureFailure("reserve_refused", code) from None
        except _LedgerDisabled:
            raise _CaptureFailure("ledger_disabled", _DISABLED) from None
        except _LedgerUnavailable:
            raise _CaptureFailure("ledger_unavailable") from None
        try:
            _candidate_id(reservation)
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
            # A replayed reservation can name a base an earlier capture declared;
            # only objects this call declared (and spooled) are uploaded.
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
                code: CandidateFailure = "upload_failed"
                if exc.code == "digest_mismatch":
                    code = "digest_mismatch"
                elif exc.code == "capture_timeout":
                    code = "capture_timeout"
                raise _CaptureFailure(code) from None
            except Exception:  # noqa: BLE001 - typed fault boundary
                raise _CaptureFailure("upload_failed") from None

    async def _complete(
        self,
        request: CandidateCaptureRequest,
        reservation: dict[str, Any],
        deadline: float,
    ) -> None:
        """Complete, retrying once after a transport fault or a 5xx.

        Completion is idempotent on a complete candidate with identical receipts,
        so a retry after a response lost in transit gets the same answer.
        """

        payload = {
            **request.authority.payload(),
            "candidate_id": str(_candidate_id(reservation)),
            "receipts": reservation["objects"],
        }
        for _attempt in range(2):
            remaining = deadline - self._clock()
            if remaining <= 0:
                break
            try:
                await _ledger_call(self._ledger.complete, payload, remaining)
                return
            except _LedgerRefused as refused:
                raise _CaptureFailure(
                    "complete_failed", refused.code or f"http_{refused.status}"
                ) from None
            except TimeoutError:
                break
            except (_LedgerUnavailable, _LedgerDisabled):
                continue
        raise _CaptureFailure("complete_failed")

    async def _failed(
        self,
        request: CandidateCaptureRequest,
        reservation: dict[str, Any] | None,
        declared: set[str],
        failure: _CaptureFailure,
    ) -> CandidateWriteResult:
        if reservation is None:
            return CandidateWriteResult("failed", None, failure.code, failure.refusal)
        candidate_id = _candidate_id(reservation)
        try:
            cleanup_complete = await asyncio.wait_for(
                self._cleanup(request, reservation, declared, failure), self._cleanup_budget
            )
        except Exception:  # noqa: BLE001 - typed fault boundary
            cleanup_complete = False
        return CandidateWriteResult(
            "failed",
            candidate_id,
            failure.code,
            failure.refusal,
            cleanup_complete,
            replayed=bool(reservation.get("replayed")),
        )

    async def _cleanup(
        self,
        request: CandidateCaptureRequest,
        reservation: dict[str, Any],
        declared: set[str],
        failure: _CaptureFailure,
    ) -> bool:
        """Tombstone the staging candidate, then discard its own declared objects.

        A writer never withdraws a complete candidate: ``fail`` is accepted only
        for a staging one, and a candidate whose ``complete`` committed with its
        response lost is refused ``candidate_state``. Objects are discarded only
        after ``fail`` confirms the tombstone; otherwise nothing is touched and the
        ledger's rows are what the collector acts on. Only the candidate's own
        patch is discarded here: a base belongs to the WorkItem's group, which
        other candidates may share, so the collector alone removes it.
        """

        keys = [
            str(obj["key"])
            for obj in reservation["objects"]
            if str(obj["kind"]) in declared and str(obj["kind"]) != "base"
        ]
        payload = {
            **request.authority.payload(),
            "candidate_id": str(_candidate_id(reservation)),
            "reason": _FAIL_REASON.get(failure.code, "upload_failed"),
        }
        try:
            response = await _ledger_call(self._ledger.fail, payload)
        except Exception:  # noqa: BLE001 - typed fault boundary
            return False
        if response.get("status") != "tombstoned":
            return False
        results = await asyncio.gather(
            *(asyncio.to_thread(self._store.discard, key) for key in keys),
            return_exceptions=True,
        )
        return not any(isinstance(item, BaseException) for item in results)

    @staticmethod
    def _outcome(
        request: CandidateCaptureRequest,
        result: CandidateWriteResult,
        sizes: dict[str, int],
    ) -> CandidateWriteResult:
        level = logging.INFO if result.status == "complete" else logging.WARNING
        logger.log(
            level,
            "publication candidate write status=%s work_item_id=%s request_id=%s "
            "candidate_id=%s sizes=%s failure=%s refusal=%s replayed=%s cleanup_complete=%s",
            result.status,
            request.authority.work_item_id,
            request.authority.request_id,
            result.candidate_id,
            ",".join(f"{kind}:{sizes[kind]}" for kind in sorted(sizes)),
            result.failure,
            result.refusal,
            result.replayed,
            result.cleanup_complete,
        )
        return result


@dataclass(frozen=True)
class CandidateObjectRef:
    kind: str
    key: str
    size_bytes: int
    sha256: str


@dataclass
class CandidateReadLease:
    lease_id: uuid.UUID
    candidate_id: uuid.UUID
    work_item_id: uuid.UUID
    holder_id: str
    expires_at: datetime
    objects: dict[str, CandidateObjectRef]
    released: bool = False


class CandidateReadRefused(Exception):
    """A refused or failed candidate read. The message is the code, never a URL."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _aware_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


class CandidateReader:
    """Reader leases plus signed, verified reads of one candidate's objects.

    A lease is bounded and nonrenewable; acquiring one never changes the
    candidate's expiry or ownership.
    """

    def __init__(
        self,
        *,
        ledger: PublicationCandidateClient,
        store: CandidateObjectStore,
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
        self, *, work_item_id: uuid.UUID, candidate_id: uuid.UUID, holder_id: str
    ) -> CandidateReadLease:
        payload = {
            "work_item_id": str(work_item_id),
            "candidate_id": str(candidate_id),
            "holder_id": holder_id,
        }
        try:
            raw = await _ledger_call(self._ledger.acquire_reader, payload)
        except _LedgerRefused as refused:
            code = refused.code or f"http_{refused.status}"
            raise CandidateReadRefused(short_code(code)) from None
        except _LedgerDisabled:
            raise CandidateReadRefused("disabled") from None
        except _LedgerUnavailable:
            raise CandidateReadRefused("ledger_unavailable") from None
        try:
            objects = {
                str(item["kind"]): CandidateObjectRef(
                    kind=str(item["kind"]),
                    key=str(item["key"]),
                    size_bytes=int(item["size_bytes"]),
                    sha256=str(item["sha256"]),
                )
                for item in raw["objects"]
            }
            lease = CandidateReadLease(
                lease_id=uuid.UUID(str(raw["lease_id"])),
                candidate_id=uuid.UUID(str(raw["candidate_id"])),
                work_item_id=work_item_id,
                holder_id=str(raw["holder_id"]),
                expires_at=_aware_utc(str(raw["expires_at"])),
                objects=objects,
            )
        except (KeyError, TypeError, ValueError):
            raise CandidateReadRefused("ledger_unavailable") from None
        logger.info(
            "publication candidate reader acquired work_item_id=%s candidate_id=%s lease_id=%s",
            work_item_id,
            lease.candidate_id,
            lease.lease_id,
        )
        return lease

    def capability(self, lease: CandidateReadLease, kind: str) -> str:
        """A signed GET for one object, expiring no later than the lease."""

        if lease.released:
            raise CandidateReadRefused("lease_released")
        remaining = math.floor(lease.expires_at.timestamp() - self._wall_clock())
        if remaining < 1:
            raise CandidateReadRefused("lease_expired")
        ref = lease.objects.get(kind)
        if ref is None:
            raise CandidateReadRefused("unknown_object")
        return self._store.presign_get(ref.key, expires_seconds=min(self._ttl, remaining))

    async def read(self, lease: CandidateReadLease, kind: str, destination: Path) -> None:
        """Stream one object to ``destination`` (mode 0600) and verify its digest."""

        url = self.capability(lease, kind)
        ref = lease.objects[kind]
        # The whole read (request, headers, body, writes, verification) must
        # finish inside the lease that authorized it.
        budget = lease.expires_at.timestamp() - self._wall_clock()
        if budget <= 0:
            raise CandidateReadRefused("lease_expired")
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
            logger.warning(
                "publication candidate read refused lease_id=%s kind=%s code=%s",
                lease.lease_id,
                kind,
                failure,
            )
            raise CandidateReadRefused(failure)

    async def release(self, lease: CandidateReadLease) -> None:
        try:
            await _ledger_call(
                self._ledger.release_reader,
                {"lease_id": str(lease.lease_id), "holder_id": lease.holder_id},
            )
        except Exception as exc:  # noqa: BLE001 - typed fault boundary
            logger.warning(
                "publication candidate reader release failed lease_id=%s cause=%s",
                lease.lease_id,
                type(exc).__name__,
            )
        finally:
            lease.released = True
