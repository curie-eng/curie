from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from aci_protocol import (
    Event,
    HookRunRef,
    QueuedTurn,
)

from ..delivery_lease import DeliveryLease
from ..hook_runs import HookRunOutcome, HookRunRecorder
from ..runner_client import (
    TurnStream,
)
from ..sandbox.types import (
    SandboxHandle,
)
from ..turn_progress import (
    activate_turn_progress,
    deactivate_turn_progress,
)

if TYPE_CHECKING:
    from .core import Kernel

from . import claim, constants, failures, memory
from .log import logger


@dataclass
class _HookRunCarry:
    """One delivery's validated hook run and actual start state."""

    recorder: HookRunRecorder | None = None
    ref: HookRunRef | None = None
    agent_id: uuid.UUID | None = None
    # A deferred slot's retry must start before this (#2929).
    retry_expires_at: datetime | None = None
    this_attempt_started: bool = False
    any_attempt_started: bool = False


def _hook_success_outcome() -> HookRunOutcome | None:
    carry = constants._HOOK_RUN_CARRY.get()
    if carry is None:
        return None
    if carry.this_attempt_started:
        return "ran"
    if carry.any_attempt_started:
        return "failed"
    return None


def _hook_failure_outcome() -> HookRunOutcome | None:
    carry = constants._HOOK_RUN_CARRY.get()
    return "failed" if carry is not None and carry.any_attempt_started else None


async def _close_hook_run_after_error(
    self: Kernel,
    qevent: QueuedTurn,
    *,
    lease: DeliveryLease | None,
    original: BaseException,
    shield: bool,
) -> None:
    carry = constants._HOOK_RUN_CARRY.get()
    if (
        carry is None
        or not carry.any_attempt_started
        or carry.recorder is None
        or carry.ref is None
        or (claim._is_fenced(lease) and lease is not None and lease.lost.is_set())
    ):
        return
    try:
        if shield:
            close_task = asyncio.create_task(
                asyncio.wait_for(
                    carry.recorder.close(carry.ref, "failed"),
                    timeout=5.0,
                )
            )
            while not close_task.done():
                try:
                    await asyncio.shield(close_task)
                except asyncio.CancelledError:
                    logger.error(
                        "hook run failure close was cancelled again for event %s; "
                        "waiting for its bounded cleanup",
                        qevent.event_id,
                    )
                    continue
            close_task.result()
        else:
            await carry.recorder.close(carry.ref, "failed")
    except BaseException:  # noqa: BLE001 - broad catch kept at a failure boundary
        logger.error(
            "hook run failure close failed for event %s while preserving %s",
            qevent.event_id,
            type(original).__name__,
            exc_info=True,
        )


async def _start_turn_under_hook_control(
    self: Kernel,
    handle: SandboxHandle,
    event: Event,
    remaining_s: float | None,
    *,
    capacity_admission: bool = False,
) -> TurnStream:
    """Serialize runner admission with the operator pause action."""

    if event.tool_access is not None:
        # @spec WORKER-TOOL-ACCESS-2: every path that opens a turn comes
        # through here, the attachment handoff included.
        await self._require_tool_access(handle, event.tool_access, remaining_s)
    extra: dict[str, Any] = {"capacity_admission": True} if capacity_admission else {}
    plan = constants._TURN_PROGRESS.get()
    progress = (
        await activate_turn_progress(
            self._progress,
            self._config,
            plan,
            answer_ref=None,
        )
        if plan is not None and self._progress is not None
        else None
    )
    if progress is not None:
        extra["progress"] = progress
    # ADR-0188: mint the memory credential here, after the claim, from the
    # same ``remaining_s`` the stream timeout is bound from.
    mint = constants._MEMORY_MINT.get()
    if mint is not None:
        event = self._with_memory_token(event, mint.qevent, mint.grant, remaining_s)
    carry = constants._HOOK_RUN_CARRY.get()
    if carry is not None and carry.recorder is not None and carry.ref is not None:
        async with carry.recorder.start_guard(carry.ref) as allowed:
            if not allowed:
                raise failures.HookPaused("cron hook paused before runner start")
            try:
                turn = await self._runner.start_turn(
                    handle.base_url,
                    event,
                    token=handle.token or None,
                    remaining_s=remaining_s,
                    **extra,
                )
            except BaseException:  # noqa: BLE001 - broad catch kept at a failure boundary
                if plan is not None and self._progress is not None:
                    await deactivate_turn_progress(self._progress, plan)
                raise
            if mint is not None:
                self._record_turn_deadline(mint.grant, remaining_s)
            memory._note_live_memory_turn(turn)
            return turn
    try:
        turn = await self._runner.start_turn(
            handle.base_url,
            event,
            token=handle.token or None,
            remaining_s=remaining_s,
            **extra,
        )
    except BaseException:  # noqa: BLE001 - broad catch kept at a failure boundary
        if plan is not None and self._progress is not None:
            await deactivate_turn_progress(self._progress, plan)
        raise
    if mint is not None:
        self._record_turn_deadline(mint.grant, remaining_s)
    # This attempt owns the turn, so it closes the steers that join it.
    memory._note_live_memory_turn(turn)
    return turn
