"""Terminal recovery capture of a failed WorkItem run's workspace (ADR 0251, #5143).

The kernel calls ``WorkspaceRecoveryCapture.capture`` once, under the thread's
lock lease, after an ordinary failed finish committed and before the run's claim
is released. One aggregate deadline covers the ledger context read, the runner
checkpoint, the base spool, the uploads and the completion. A failure that ends
the capture before any reservation is recorded through the ledger's ``outcome``
operation within a separate cleanup allowance, so a capture that spent its whole
budget still records why.

Nothing here raises to the caller and nothing changes the WorkItem's terminal
outcome: the finish already committed. Logs carry ids, codes and sizes only,
never captured bytes, signed URLs or tokens.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

import httpx

from .runner_client import RunnerCheckpointRefused, RunnerClient, RunnerError
from .sandbox import SandboxHandle
from .workitem_dispatch import WorkItemRun
from .workspace import PreparedWorkspace, WorkspaceClaimCoordinator
from .workspace_recovery import WorkspaceRecoveryClient
from .workspace_recovery_storage import (
    CAPTURE_BUDGET_SECONDS,
    CLEANUP_BUDGET_SECONDS,
    RecoveryAuthority,
    RecoveryBaseSource,
    RecoveryCaptureRequest,
    RecoveryCheckpointWriter,
    RecoveryRepository,
)

logger = logging.getLogger(__name__)

# The runner gets slightly less than what is left, so its own coded deadline
# answer arrives before this side's request timeout.
_RUNNER_MARGIN_SECONDS = 1.0

# A coded runner refusal and the WorkItem outcome it records.
_REFUSAL_OUTCOMES: dict[str, str] = {
    "workspace_changed": "torn_capture",
    "patch_too_large": "patch_too_large",
    "manifest_too_large": "manifest_too_large",
    "unsafe_entry": "capture_failed",
    "unreadable": "capture_failed",
    "base_unavailable": "capture_failed",
    "deadline_exceeded": "capture_timeout",
}

# A writer failure that ended before any reservation and the WorkItem outcome it
# records. Ledger refusals (``reserve_refused`` and the ``*_too_large`` codes)
# recorded their own outcome and are absent here.
_UNRESERVED_OUTCOMES: dict[str, str] = {
    "capture_timeout": "capture_timeout",
    "base_unavailable": "capture_failed",
    "base_digest_mismatch": "capture_failed",
    "upload_failed": "capture_failed",
    "ledger_unavailable": "capture_failed",
}


@dataclass(frozen=True)
class RecoveryTerminalResult:
    status: Literal["complete", "failed", "skipped"]
    code: str | None = None
    checkpoint_id: uuid.UUID | None = None


class _Ended(Exception):
    """The capture ended before any reservation, with this outcome code."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class _Skipped(Exception):
    """The ledger cannot or will not start a capture; nothing is recorded."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _refusal_code(exc: httpx.HTTPStatusError) -> str:
    try:
        body = exc.response.json()
    except ValueError:
        body = None
    detail = body.get("detail") if isinstance(body, dict) else None
    code = detail.get("code") if isinstance(detail, dict) else None
    return code if isinstance(code, str) else f"http_{exc.response.status_code}"


class WorkspaceRecoveryCapture:
    """Context read, runner checkpoint and checkpoint write under one deadline."""

    def __init__(
        self,
        *,
        ledger: WorkspaceRecoveryClient,
        writer: RecoveryCheckpointWriter,
        runner: RunnerClient,
        coordinator: WorkspaceClaimCoordinator,
        budget_seconds: float = CAPTURE_BUDGET_SECONDS,
        cleanup_budget_seconds: float = CLEANUP_BUDGET_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not 0 < budget_seconds <= CAPTURE_BUDGET_SECONDS:
            raise ValueError("recovery capture budget must be in (0, 60] seconds")
        self._ledger = ledger
        self._writer = writer
        self._runner = runner
        self._coordinator = coordinator
        self._budget = budget_seconds
        self._cleanup_budget = cleanup_budget_seconds
        self._clock = clock

    async def capture(
        self,
        run: WorkItemRun,
        handle: SandboxHandle,
        prepared: PreparedWorkspace,
        *,
        fence: Callable[[], Awaitable[None]],
    ) -> RecoveryTerminalResult:
        """Capture the run's workspace. Never raises; never changes the run's outcome."""

        deadline = self._clock() + self._budget
        if run.runtime_epoch is None:
            return self._log(run, RecoveryTerminalResult("skipped", "no_runtime_epoch"))
        authority = RecoveryAuthority(
            work_item_id=run.work_item_id,
            source_request_id=run.request_id,
            runtime_epoch=run.runtime_epoch,
            dispatch_generation=run.generation,
        )
        try:
            result = await self._capture(run, handle, prepared, authority, deadline, fence)
        except _Skipped as skipped:
            result = RecoveryTerminalResult("skipped", skipped.code)
        except _Ended as ended:
            await self._record(authority, ended.code)
            result = RecoveryTerminalResult("failed", ended.code)
        except Exception as exc:  # noqa: BLE001 - a capture fault never reaches the kernel
            result = RecoveryTerminalResult("failed", f"error_{type(exc).__name__}")
        return self._log(run, result)

    async def _capture(
        self,
        run: WorkItemRun,
        handle: SandboxHandle,
        prepared: PreparedWorkspace,
        authority: RecoveryAuthority,
        deadline: float,
        fence: Callable[[], Awaitable[None]],
    ) -> RecoveryTerminalResult:
        context = await self._context(authority, deadline)
        repository = context["repository"]
        if prepared.base_sha != context["base_commit"]:
            raise _Ended("base_mismatch")
        remaining = deadline - self._clock()
        if remaining <= 0:
            raise _Ended("capture_timeout")
        try:
            checkpoint = await self._runner.checkpoint(
                handle.base_url,
                handle.token,
                base_sha=prepared.base_sha,
                budget_seconds=max(remaining - _RUNNER_MARGIN_SECONDS, remaining / 2),
                remaining_s=remaining,
            )
        except RunnerCheckpointRefused as refused:
            raise _Ended(_REFUSAL_OUTCOMES.get(refused.code, "runner_unavailable")) from None
        except TimeoutError:
            raise _Ended("capture_timeout") from None
        except RunnerError:
            raise _Ended("runner_unavailable") from None
        written = await self._writer.capture(
            RecoveryCaptureRequest(
                authority=authority,
                idempotency_key=f"{run.request_id}:{run.runtime_epoch}",
                repository=RecoveryRepository(
                    provider=str(repository["provider"]),
                    host=str(repository["host"]),
                    project_id=str(repository["project_id"]),
                    path=str(repository["path"]),
                ),
                base_commit=prepared.base_sha,
                target_branch=str(context["target_branch"]),
                patch=checkpoint.patch,
                manifest=checkpoint.manifest,
                # Built from the claim the kernel already read: nothing here touches
                # the store on the event loop; the writer opens it in its spool thread.
                base=RecoveryBaseSource.from_prepared(self._coordinator, prepared),
            ),
            deadline=deadline,
            fence=fence,
        )
        if written.status == "complete":
            return RecoveryTerminalResult("complete", None, written.checkpoint_id)
        if written.checkpoint_id is None and written.failure in _UNRESERVED_OUTCOMES:
            # The writer failed before any reservation, so no ledger operation has
            # recorded why; the cleanup allowance records it now.
            await self._record(authority, _UNRESERVED_OUTCOMES[written.failure])
        return RecoveryTerminalResult("failed", written.failure, written.checkpoint_id)

    async def _context(self, authority: RecoveryAuthority, deadline: float) -> dict[str, Any]:
        remaining = deadline - self._clock()
        if remaining <= 0:
            raise _Ended("capture_timeout")
        try:
            context = await asyncio.wait_for(self._ledger.context(authority.payload()), remaining)
        except TimeoutError:
            raise _Ended("capture_timeout") from None
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code < 500:
                raise _Skipped(_refusal_code(exc)) from None
            raise _Skipped("ledger_unavailable") from None
        except (httpx.HTTPError, ValueError):
            raise _Skipped("ledger_unavailable") from None
        repository = context.get("repository")
        if (
            not isinstance(repository, dict)
            or not isinstance(context.get("base_commit"), str)
            or not isinstance(context.get("target_branch"), str)
        ):
            raise _Skipped("ledger_unavailable")
        return context

    async def _record(self, authority: RecoveryAuthority, reason: str) -> None:
        """Record a pre-reservation outcome inside the cleanup allowance."""

        try:
            await asyncio.wait_for(
                self._ledger.outcome({**authority.payload(), "reason": reason}),
                self._cleanup_budget,
            )
        except Exception as exc:  # noqa: BLE001 - recording is best effort
            logger.warning(
                "workspace recovery outcome not recorded work_item_id=%s request_id=%s "
                "reason=%s cause=%s",
                authority.work_item_id,
                authority.source_request_id,
                reason,
                type(exc).__name__,
            )

    @staticmethod
    def _log(run: WorkItemRun, result: RecoveryTerminalResult) -> RecoveryTerminalResult:
        logger.info(
            "workspace recovery terminal capture status=%s code=%s work_item_id=%s "
            "request_id=%s checkpoint_id=%s",
            result.status,
            result.code,
            run.work_item_id,
            run.request_id,
            result.checkpoint_id,
        )
        return result
