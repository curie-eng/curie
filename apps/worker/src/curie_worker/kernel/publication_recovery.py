"""Publish a retained candidate with no model turn (ADR 0269, #5429).

A ``publication_recovery`` request is an ordinary admitted request: the
kernel acquires and starts it through the dispatch API (a fresh runtime epoch
and execution deadline) and claims and boots the ordinary runner sandbox on
the WorkItem's managed workspace. The route opens no runner turn. Instead,
once the runner reports ready (its declared verification preflight finished),
the request's one candidate is read back exactly, checked against the fresh
trusted base, validated as any snapshot is, and handed to the ordinary
publication pause, which creates the publication under the request's own
epoch with the execute event as its dedupe key.

No route here opens a model session, a turn or a generating call: the runner
is asked only for ``/v1/status``.

A refusal (changed base, missing or unreadable candidate, a receipt that does
not match, a policy refusal) finishes the request failed with the terminal
cause ``publication_recovery_refused`` and the code as its detail. An
infrastructure fault (the runner never ready, the ledger or the store
unavailable, a validation fault) becomes the ordinary publication snapshot
failure, so the pause finishes it ``approval_create_failed`` exactly as a
snapshot read failure does. Logs carry ids and codes, never bytes or URLs.
"""

from __future__ import annotations

import asyncio
import hashlib
import shutil
import tempfile
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiohttp
import httpx
from aci_protocol import QueuedTurn, SessionStatus

from ..publication_candidate_storage import (
    CandidateAuthority,
    CandidateReader,
    CandidateReadRefused,
    refusal_code,
    short_code,
)
from ..publication_validation import PublicationValidationRefused, validate_snapshot_against_base
from ..runner_client import RunnerError, RunnerWorkspaceSnapshot
from ..sandbox.types import SandboxHandle
from ..workitem_dispatch import WorkItemConflict, WorkItemRun
from ..workspace import WorkspacePreparationError
from . import clock, constants, failures
from .log import logger

if TYPE_CHECKING:
    from .core import Kernel

RECOVERY_REFUSED_CAUSE = "publication_recovery_refused"
# The runner status is polled at this interval until it reports ready.
_READY_POLL_S = 0.5
# One status read and one ledger call are bounded by these control-plane caps.
_STATUS_READ_CAP_S = 2.0
_LEDGER_CALL_CAP_S = 10.0


class _PublicationRecoveryRefused(Exception):
    """The candidate cannot be published as it stands; terminal, never retried."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class _RecoveryUnavailable(Exception):
    """An infrastructure fault; the ordinary publication failure path classifies it."""


@dataclass(frozen=True)
class _PatchReceipt:
    key: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class _Recovery:
    """The recovery route's answer, parsed strictly."""

    candidate_id: uuid.UUID
    repository_path: str
    base_commit: str
    changed_paths: tuple[str, ...]
    contains_workflow_files: bool
    title: str
    body: str
    patch: _PatchReceipt


def _parse_recovery(body: dict[str, Any]) -> _Recovery:
    """The route's metadata; anything malformed is an unusable answer."""

    try:
        repository = body["repository"]
        patch = body["patch"]
        paths = body["changed_paths"]
        if (
            not isinstance(repository, dict)
            or not isinstance(patch, dict)
            or not isinstance(paths, list)
            or not all(isinstance(path, str) for path in paths)
            or type(body["contains_workflow_files"]) is not bool
            or not isinstance(body["title"], str)
            or not isinstance(body["body"], str)
            or patch.get("kind") != "patch"
            or type(patch.get("size_bytes")) is not int
        ):
            raise ValueError("malformed recovery metadata")
        recovery = _Recovery(
            candidate_id=uuid.UUID(str(body["candidate_id"])),
            repository_path=_text(repository["path"]),
            base_commit=_text(body["base_commit"]),
            changed_paths=tuple(paths),
            contains_workflow_files=body["contains_workflow_files"],
            title=body["title"],
            body=body["body"],
            patch=_PatchReceipt(
                key=_text(patch["key"]),
                size_bytes=patch["size_bytes"],
                sha256=_text(patch["sha256"]),
            ),
        )
    except (KeyError, TypeError, ValueError):
        raise _RecoveryUnavailable("the recovery route returned unusable metadata") from None
    return recovery


def _text(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("required string missing")
    return value


def _budget(run: WorkItemRun, remaining_s: float | None) -> Callable[[], float | None]:
    """What is left of the delivery budget, bounded by the execution deadline."""

    started = clock.time.monotonic()

    def left() -> float | None:
        delivery = None if remaining_s is None else remaining_s - (clock.time.monotonic() - started)
        return run.bound_remaining_s(delivery)

    return left


def _bounded(left: float | None, cap: float) -> float:
    if left is None:
        return cap
    if left <= 0:
        raise _RecoveryUnavailable("the recovery budget is spent")
    return min(cap, left)


async def _await_ready(
    self: Kernel, handle: SandboxHandle, left: Callable[[], float | None]
) -> None:
    """Poll the runner's status until its boot (and verification preflight) finished.

    Only ``/v1/status`` is read: nothing here opens a model session or a turn.
    """

    while True:
        remaining = left()
        if remaining is not None and remaining <= 0:
            raise _RecoveryUnavailable("the validation runtime never reported ready")
        try:
            status = await self._runner.status(
                handle.base_url,
                token=handle.token or None,
                remaining_s=_bounded(remaining, _STATUS_READ_CAP_S),
            )
        except (RunnerError, aiohttp.ClientError, TimeoutError, OSError) as exc:
            logger.info(
                "publication recovery runner status unavailable at %s: %s",
                handle.claim_name,
                type(exc).__name__,
            )
        else:
            if status.get("ready") is True:
                return
        remaining = left()
        await asyncio.sleep(_READY_POLL_S if remaining is None else min(_READY_POLL_S, remaining))


async def _route_metadata(
    reader: CandidateReader, run: WorkItemRun, left: Callable[[], float | None]
) -> _Recovery:
    """Ask the API for the request's candidate under its live authority."""

    assert run.runtime_epoch is not None
    authority = CandidateAuthority(
        work_item_id=run.work_item_id,
        request_id=run.request_id,
        runtime_epoch=run.runtime_epoch,
        dispatch_generation=run.generation,
    )
    try:
        body = await asyncio.wait_for(
            reader.ledger.recovery(authority.payload()), _bounded(left(), _LEDGER_CALL_CAP_S)
        )
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 409:
            code = refusal_code(exc.response)
            raise _PublicationRecoveryRefused(
                short_code(code) if code else "recovery_refused"
            ) from None
        raise _RecoveryUnavailable(
            f"the recovery route answered HTTP {exc.response.status_code}"
        ) from None
    except (httpx.HTTPError, TimeoutError, ValueError) as exc:
        raise _RecoveryUnavailable(
            f"the recovery route is unavailable: {type(exc).__name__}"
        ) from None
    return _parse_recovery(body)


async def _read_patch(
    self: Kernel,
    reader: CandidateReader,
    run: WorkItemRun,
    recovery: _Recovery,
    left: Callable[[], float | None],
) -> bytes:
    """Lease the candidate and read its patch, which must equal the route's receipt.

    The bytes land in a private file under the workspace scratch root; the
    lease is always released and the file always removed.
    """

    try:
        lease = await asyncio.wait_for(
            reader.acquire(
                work_item_id=run.work_item_id,
                candidate_id=recovery.candidate_id,
                holder_id=f"recovery-{uuid.uuid4().hex[:12]}",
            ),
            _bounded(left(), _LEDGER_CALL_CAP_S),
        )
    except TimeoutError:
        raise _RecoveryUnavailable("the candidate reader lease timed out") from None
    scratch: Path | None = None
    try:
        ref = lease.objects.get("patch")
        if ref is None or (ref.key, ref.size_bytes, ref.sha256) != (
            recovery.patch.key,
            recovery.patch.size_bytes,
            recovery.patch.sha256,
        ):
            raise _PublicationRecoveryRefused("patch_receipt_mismatch")
        root = Path(self._config.workspace_scratch_root)
        scratch = Path(
            await asyncio.to_thread(tempfile.mkdtemp, prefix="publication-recovery-", dir=root)
        )
        destination = scratch / "patch"
        await reader.read(lease, "patch", destination)
        patch = await asyncio.to_thread(destination.read_bytes)
    finally:
        await reader.release(lease)
        if scratch is not None:
            await asyncio.to_thread(shutil.rmtree, scratch, True)
    if len(patch) != recovery.patch.size_bytes or (
        hashlib.sha256(patch).hexdigest() != recovery.patch.sha256
    ):
        raise _PublicationRecoveryRefused("digest_mismatch")
    return patch


async def _recover_publication(
    self: Kernel,
    qevent: QueuedTurn,
    thread_key: str,
    handle: SandboxHandle,
    run: WorkItemRun,
    remaining_s: float | None,
) -> failures.TurnOutcome:
    """Validate the request's retained candidate and return it as a publish outcome.

    Raises ``_PublicationRecoveryRefused`` for a terminal refusal. Returns a
    publication outcome whose snapshot error names an infrastructure fault,
    which the pause reports as the ordinary publication failure.
    """

    left = _budget(run, remaining_s)
    reader = self._publication_candidate_reader
    candidate_id = run.recovery_candidate_id
    if (
        reader is None
        or self._workspace is None
        or candidate_id is None
        or run.runtime_epoch is None
    ):
        raise _PublicationRecoveryRefused("recovery_unavailable")
    try:
        await _await_ready(self, handle, left)
        recovery = await _route_metadata(reader, run, left)
        if recovery.candidate_id != candidate_id:
            raise _PublicationRecoveryRefused("candidate_mismatch")
        prepared = await asyncio.to_thread(self._workspace.current, thread_key)
        if prepared is None:
            raise _RecoveryUnavailable("thread has no retained sanitized base")
        if (
            prepared.base_sha != recovery.base_commit
            or prepared.repo_full_name.casefold() != recovery.repository_path.casefold()
        ):
            raise _PublicationRecoveryRefused("base_changed")
        try:
            patch = await _read_patch(self, reader, run, recovery, left)
        except CandidateReadRefused as exc:
            if exc.code == "ledger_unavailable":
                raise _RecoveryUnavailable("the candidate ledger is unavailable") from None
            raise _PublicationRecoveryRefused(exc.code) from None
        snapshot = RunnerWorkspaceSnapshot(
            repo_full_name=recovery.repository_path,
            base_sha=recovery.base_commit,
            patch=patch,
            changed_paths=recovery.changed_paths,
            contains_workflow_files=recovery.contains_workflow_files,
            publication_title=recovery.title,
            publication_body=recovery.body,
        )
        try:
            # Exactly the validation every snapshot passes, against the fresh
            # trusted base; no candidate is captured again.
            await asyncio.to_thread(
                validate_snapshot_against_base,
                self._workspace,
                thread_key=thread_key,
                snapshot=snapshot,
                max_patch_bytes=self._config.publication_patch_max_bytes,
                scratch_root=Path(self._config.workspace_scratch_root),
                git_timeout_seconds=self._config.publication_git_command_timeout_seconds,
                protected_paths=self._config.publication_protected_paths,
                allow_dependency_additions=self._config.publication_allow_dependency_additions,
            )
        except PublicationValidationRefused:
            raise _PublicationRecoveryRefused("validation_refused") from None
        except WorkspacePreparationError as exc:
            raise _RecoveryUnavailable(str(exc)) from None
    except _RecoveryUnavailable as exc:
        logger.warning(
            "publication recovery unavailable work_item_id=%s request_id=%s candidate_id=%s: %s",
            run.work_item_id,
            run.request_id,
            candidate_id,
            exc,
        )
        return _publish_outcome(None, f"publication recovery failed: {exc}")
    logger.info(
        "publication recovery validated work_item_id=%s request_id=%s candidate_id=%s "
        "event=%s patch_bytes=%d",
        run.work_item_id,
        run.request_id,
        candidate_id,
        qevent.event_id,
        len(snapshot.patch),
    )
    return _publish_outcome(snapshot, None)


def _publish_outcome(
    snapshot: RunnerWorkspaceSnapshot | None, error: str | None
) -> failures.TurnOutcome:
    """The outcome a runner publish request ends in, with the given snapshot or error."""

    gate_kind, granted_tool = constants._PUBLISH_PROVENANCE
    return failures.TurnOutcome(
        terminal_ok=True,
        status=SessionStatus.AWAITING_APPROVAL,
        approval_gate_kind=gate_kind,
        approval_granted_tool=granted_tool,
        approval_summary=snapshot.publication_title if snapshot is not None else None,
        publication_snapshot=snapshot,
        publication_snapshot_error=error,
        # No model ran: the kernel observed everything this delivery did.
        runner_ended=True,
    )


async def _attempt_recovery(
    self: Kernel,
    qevent: QueuedTurn,
    thread_key: str,
    handle: SandboxHandle,
    remaining_s: float | None,
) -> failures.TurnOutcome:
    """The attempt of a recovery route, which opened no runner turn.

    A refusal finishes the run ``publication_recovery_refused`` here and
    completes the event without any model; everything else is the pause's.
    """

    owned_id = constants._OWNED_WORK_ITEM.get()
    run = self._work_item_runs.get(owned_id) if owned_id is not None else None
    if run is None or run.event_id != qevent.event_id or not run.is_publication_recovery:
        raise RunnerError("a route without a turn has no publication recovery run")
    try:
        async with self._keep_route_alive(thread_key, handle.claim_name):
            return await self._recover_publication(qevent, thread_key, handle, run, remaining_s)
    except _PublicationRecoveryRefused as refused:
        logger.warning(
            "publication recovery refused work_item_id=%s request_id=%s candidate_id=%s code=%s",
            run.work_item_id,
            run.request_id,
            run.recovery_candidate_id,
            refused.code,
        )
        try:
            await self._finish_or_settle(
                run,
                outcome="failed",
                cause=RECOVERY_REFUSED_CAUSE,
                detail=refused.code,
                ci_fix_round=None,
                retry_safety="no_effects",
                retry_condition=None,
                exhausted_volume=None,
            )
        except WorkItemConflict as exc:
            # As at the approval_create_failed finish: the request already
            # settled (cancelled) or the publication loop owns its terminus.
            if exc.code not in {"publication_pending", "work_item_cancelled"}:
                raise
            run.finished = True
        return failures.TurnOutcome(terminal_ok=True, start_failed=True, runner_ended=True)
