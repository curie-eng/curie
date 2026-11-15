"""Retain a publication snapshot as a private candidate before validation (#5428).

The kernel calls ``PublicationCandidateCapture.capture`` once per publication
snapshot of a started WorkItem run, after the runner snapshot was read and
before ``validate_snapshot_against_base``. One aggregate deadline, at most 60 s,
covers the ledger context read, the base spool, the uploads and the completion.
A failure that ends the capture before any reservation is recorded through the
ledger's ``outcome`` operation within a separate cleanup allowance, so a capture
that spent its whole budget still records why.

Nothing here raises to the caller and nothing changes the publication outcome:
validation runs exactly as it would without a candidate. A candidate grants no
publication authority. Logs carry ids, codes and sizes only, never bytes, signed
URLs, tokens, the publication title or body, or paths.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

import httpx

from .publication_candidate_storage import (
    CAPTURE_BUDGET_SECONDS,
    CLEANUP_BUDGET_SECONDS,
    MAX_CHANGED_PATH_CHARS,
    MAX_CHANGED_PATHS,
    CandidateAuthority,
    CandidateCaptureRequest,
    CandidateRepository,
    CandidateWriter,
    refusal_code,
    short_code,
)
from .publication_candidates import PublicationCandidateClient
from .runner_client import RunnerWorkspaceSnapshot
from .workitem_dispatch import WorkItemRun
from .workspace import PreparedWorkspace, WorkspaceClaimCoordinator
from .workspace_recovery_storage import RecoveryBaseSource

logger = logging.getLogger(__name__)

_DISABLED = "publication_candidates.disabled"

# A writer failure that ended before any reservation, with no ledger answer
# behind it, and the outcome reason it records. A ledger refusal recorded its
# own outcome (or was refused authority to) and is absent here.
_UNRESERVED_OUTCOMES: dict[str, str] = {
    "capture_timeout": "capture_timeout",
    "base_unavailable": "base_unavailable",
    "base_digest_mismatch": "base_digest_mismatch",
    "upload_failed": "upload_failed",
    "ledger_unavailable": "ledger_unavailable",
}


@dataclass(frozen=True)
class CandidateCaptureResult:
    status: Literal["complete", "failed", "skipped"]
    code: str | None = None
    candidate_id: uuid.UUID | None = None


class _Ended(Exception):
    """The capture ended before any reservation; record this outcome reason."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class _Refused(Exception):
    """The ledger refused the capture's authority; nothing can be recorded."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class _Skipped(Exception):
    """The ledger will not retain candidates; nothing is recorded."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class PublicationCandidateCapture:
    """Context read and candidate write under one deadline. Never raises."""

    def __init__(
        self,
        *,
        ledger: PublicationCandidateClient,
        writer: CandidateWriter,
        coordinator: WorkspaceClaimCoordinator,
        budget_seconds: float = CAPTURE_BUDGET_SECONDS,
        cleanup_budget_seconds: float = CLEANUP_BUDGET_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not 0 < budget_seconds <= CAPTURE_BUDGET_SECONDS:
            raise ValueError("candidate capture budget must be in (0, 60] seconds")
        self._ledger = ledger
        self._writer = writer
        self._coordinator = coordinator
        self._budget = budget_seconds
        self._cleanup_budget = cleanup_budget_seconds
        self._clock = clock

    async def capture(
        self,
        run: WorkItemRun,
        snapshot: RunnerWorkspaceSnapshot,
        prepared: PreparedWorkspace,
        *,
        dedupe_key: str,
        budget_seconds: float,
    ) -> CandidateCaptureResult:
        """Retain ``snapshot`` and its trusted base. Never raises."""

        try:
            if run.runtime_epoch is None:
                return self._log(run, CandidateCaptureResult("skipped", "no_runtime_epoch"))
            if not snapshot.patch:
                return self._log(run, CandidateCaptureResult("skipped", "no_patch"))
            authority = CandidateAuthority(
                work_item_id=run.work_item_id,
                request_id=run.request_id,
                runtime_epoch=run.runtime_epoch,
                dispatch_generation=run.generation,
            )
        except Exception as exc:  # noqa: BLE001 - a capture fault never reaches the kernel
            return self._log(run, CandidateCaptureResult("failed", f"error_{type(exc).__name__}"))
        budget = min(budget_seconds, self._budget, CAPTURE_BUDGET_SECONDS)
        deadline = self._clock() + budget
        try:
            result = await self._capture(snapshot, prepared, authority, dedupe_key, deadline)
        except _Skipped as skipped:
            result = CandidateCaptureResult("skipped", skipped.code)
        except _Refused as refused:
            result = CandidateCaptureResult("failed", refused.code)
        except _Ended as ended:
            await self._record(authority, ended.code)
            result = CandidateCaptureResult("failed", ended.code)
        except Exception as exc:  # noqa: BLE001 - a capture fault never reaches the kernel
            result = CandidateCaptureResult("failed", f"error_{type(exc).__name__}")
        return self._log(run, result, len(snapshot.patch))

    async def _capture(
        self,
        snapshot: RunnerWorkspaceSnapshot,
        prepared: PreparedWorkspace,
        authority: CandidateAuthority,
        dedupe_key: str,
        deadline: float,
    ) -> CandidateCaptureResult:
        if deadline - self._clock() <= 0:
            raise _Ended("capture_timeout")
        context = await self._context(authority, deadline)
        repository = context["repository"]
        # One trusted base: the runner's anchor, the claim's prepared commit and
        # the WorkItem's recorded base must all be the same commit.
        if not (snapshot.base_sha == prepared.base_sha == context["base_commit"]):
            raise _Ended("base_mismatch")
        path = str(repository["path"]).casefold()
        if not (path == prepared.repo_full_name.casefold() == snapshot.repo_full_name.casefold()):
            raise _Ended("base_mismatch")
        # The ledger's finite bounds on retained metadata, checked before any
        # reservation so an unbounded list is a recorded failure, not a 422.
        if len(snapshot.changed_paths) > MAX_CHANGED_PATHS or any(
            not 0 < len(changed) <= MAX_CHANGED_PATH_CHARS for changed in snapshot.changed_paths
        ):
            raise _Ended("capture_failed")
        written = await self._writer.capture(
            CandidateCaptureRequest(
                authority=authority,
                dedupe_key=dedupe_key,
                repository=CandidateRepository(
                    provider=str(repository["provider"]),
                    host=str(repository["host"]),
                    project_id=str(repository["project_id"]),
                    path=str(repository["path"]),
                ),
                base_commit=prepared.base_sha,
                target_branch=str(context["target_branch"]),
                patch=snapshot.patch,
                changed_paths=snapshot.changed_paths,
                contains_workflow_files=snapshot.contains_workflow_files,
                title=snapshot.publication_title,
                body=snapshot.publication_body,
                # Built from the claim the kernel already read: nothing here touches
                # the store on the event loop; the writer opens it in its spool thread.
                base=RecoveryBaseSource.from_prepared(self._coordinator, prepared),
            ),
            deadline=deadline,
        )
        if written.status == "complete":
            return CandidateCaptureResult("complete", None, written.candidate_id)
        if written.refusal == _DISABLED:
            return CandidateCaptureResult("skipped", "disabled")
        if written.refusal is not None:
            # A ledger refusal: recorded refusals (capacity, size, base conflict)
            # already committed the WorkItem's outcome row.
            return CandidateCaptureResult(
                "failed", short_code(written.refusal), written.candidate_id
            )
        failure = written.failure or "capture_failed"
        if written.candidate_id is None and failure in _UNRESERVED_OUTCOMES:
            # The writer failed before any reservation, so no ledger operation has
            # recorded why; the cleanup allowance records it now.
            await self._record(authority, _UNRESERVED_OUTCOMES[failure])
        return CandidateCaptureResult("failed", failure, written.candidate_id)

    async def _context(self, authority: CandidateAuthority, deadline: float) -> dict[str, Any]:
        remaining = deadline - self._clock()
        if remaining <= 0:
            raise _Ended("capture_timeout")
        try:
            context = await asyncio.wait_for(self._ledger.context(authority.payload()), remaining)
        except TimeoutError:
            raise _Ended("capture_timeout") from None
        except httpx.HTTPStatusError as exc:
            code = refusal_code(exc.response)
            if exc.response.status_code == 503 and code == _DISABLED:
                raise _Skipped("disabled") from None
            if exc.response.status_code < 500:
                raise _Refused(
                    short_code(code) if code else f"http_{exc.response.status_code}"
                ) from None
            raise _Ended("ledger_unavailable") from None
        except (httpx.HTTPError, ValueError):
            raise _Ended("ledger_unavailable") from None
        repository = context.get("repository")
        if (
            not isinstance(repository, dict)
            or not isinstance(context.get("base_commit"), str)
            or not isinstance(context.get("target_branch"), str)
            or not all(
                isinstance(repository.get(name), str)
                for name in ("provider", "host", "project_id", "path")
            )
        ):
            raise _Ended("ledger_unavailable")
        return context

    async def _record(self, authority: CandidateAuthority, reason: str) -> None:
        """Record a pre-reservation outcome inside the cleanup allowance."""

        try:
            await asyncio.wait_for(
                self._ledger.outcome({**authority.payload(), "reason": reason}),
                self._cleanup_budget,
            )
        except Exception as exc:  # noqa: BLE001 - recording is best effort
            logger.warning(
                "publication candidate outcome not recorded work_item_id=%s request_id=%s "
                "reason=%s cause=%s",
                authority.work_item_id,
                authority.request_id,
                reason,
                type(exc).__name__,
            )

    @staticmethod
    def _log(
        run: WorkItemRun, result: CandidateCaptureResult, patch_bytes: int | None = None
    ) -> CandidateCaptureResult:
        level = logging.WARNING if result.status == "failed" else logging.INFO
        logger.log(
            level,
            "publication candidate capture status=%s code=%s work_item_id=%s request_id=%s "
            "candidate_id=%s patch_bytes=%s",
            result.status,
            result.code,
            run.work_item_id,
            run.request_id,
            result.candidate_id,
            patch_bytes,
        )
        return result
