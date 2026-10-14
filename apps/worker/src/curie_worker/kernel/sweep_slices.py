"""A long scheduled sweep's slice boundary and stop notice (ADR-0160, #2878).

A targeted cron delivery cut by its delivery budget continues as the next
delivery of the same hook run, on the same live sandbox and session, when the
agent's newest ``sweep-checkpoint`` fact still lists uncovered sources and
shows progress. A sweep that stops short instead posts one coverage notice.
The checkpoint parser, the continuation ids and the coverage read live in
``curie_worker.sweep``; this module is only the kernel's use of them.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from aci_protocol import QueuedTurn, TurnSource

from ..delivery_lease import DeliveryLease
from ..hook_runs import HookRunOutcome, HookRunRecorderError
from ..reply_sink import TargetRoute
from ..sweep import (
    MAX_STALLED_SLICES,
    MAX_SWEEP_SLICES,
    SweepRead,
    SweepRun,
    continuation_event_id,
    coverage_notice_text,
    distinct_covered,
    parse_continuation,
)

if TYPE_CHECKING:
    from .core import Kernel

from . import constants, failures, routing
from .log import logger

# Only a run that stopped short owes a notice; ``ran`` is the agent's own post.
_NOTICE_OUTCOMES: frozenset[str] = frozenset({"failed", "skipped", "blocked"})


def _sweep_run(self: Kernel, qevent: QueuedTurn) -> SweepRun | None:
    """This delivery's hook run as a coverage read needs it, or None."""
    carry = constants._HOOK_RUN_CARRY.get()
    if self._sweep is None or carry is None or carry.state is None:
        return None
    state = carry.state
    handle = qevent.reply_handle
    return SweepRun(
        agent_id=state.agent_id,
        hook=state.name,
        author=qevent.author,
        slot_utc=state.slot_utc,
        started_at=state.started_at,
        version_id=state.version_id,
        binding=(handle.kind, handle.channel) if handle is not None else None,
    )


async def _sweep_read(self: Kernel, qevent: QueuedTurn) -> SweepRead:
    """One coverage read per delivery, cached on the hook carry. Never raises."""
    carry = constants._HOOK_RUN_CARRY.get()
    if carry is not None and carry.sweep_read is not None:
        return carry.sweep_read
    run = self._sweep_run(qevent)
    if run is None or self._sweep is None:
        return SweepRead(date=None, checkpoint=None)
    read = await self._sweep.read(run)
    if carry is not None:
        carry.sweep_read = read
    return read


async def _continue_sweep(
    self: Kernel,
    qevent: QueuedTurn,
    route: TargetRoute,
    lease: DeliveryLease,
    outcome: failures.TurnOutcome,
) -> bool:
    """Queue the next slice of a budget-cut sweep; True when this delivery is settled.

    False leaves the caller's existing terminal handling in charge, which
    closes the run and lets ``_complete`` post the coverage notice.
    """
    # Renew before the read: the lease was last renewed at this delivery's
    # start and lapses about when the budget does, after which the hook's next
    # fire may reclaim the row. A row that can no longer renew is terminal, and
    # whoever closed it owns its report. With no hook run carry, ``_renew`` is
    # False too.
    if not await _renew(self, qevent):
        return False
    read = await self._sweep_read(qevent)
    checkpoint = read.checkpoint
    if checkpoint is None or not checkpoint.uncovered:
        return False
    covered = distinct_covered(checkpoint)
    carried = parse_continuation(qevent.event_id)
    stalled = 0
    if carried is not None and covered <= carried[2]:
        # No new source this slice. One source may outlast a budget cut or
        # two; a sweep that stays stuck stops rather than holding the open row
        # and keeping every later fire out.
        stalled = carried[3] + 1
        if stalled >= MAX_STALLED_SLICES:
            logger.info("sweep %s stopped: no new source in %d slices", qevent.event_id, stalled)
            return False
    next_slice = 1 if carried is None else carried[1] + 1
    if next_slice > MAX_SWEEP_SLICES:
        logger.info("sweep %s stopped: it reached %d slices", qevent.event_id, MAX_SWEEP_SLICES)
        return False
    if lease.lost.is_set():
        # An early exit only; the fenced settle below is the real check.
        logger.warning(
            "delivery lease lost at the sweep slice boundary for event %s; "
            "publishing nothing and settling nothing",
            qevent.event_id,
        )
        return True
    if not await _renew(self, qevent):
        return False
    successor = qevent.model_copy(
        update={
            "event_id": continuation_event_id(qevent.event_id, covered, stalled),
            "received_at": datetime.now(UTC).isoformat(),
        }
    )
    logger.info(
        "sweep slice %s cut at its delivery budget; continuing as %s (covered=%d uncovered=%d)",
        qevent.event_id,
        successor.event_id,
        covered,
        len(checkpoint.uncovered),
    )
    # The successor is published in the same fenced script that settles this
    # slice, so a fenced-out owner publishes nothing. The run row stays open.
    await self._complete(
        qevent,
        route,
        "dropped",
        telemetry_outcome="deadline_halted",
        lease=lease,
        hook_outcome=None,
        turn=outcome,
        successor=successor,
    )
    return True


async def _renew(self: Kernel, qevent: QueuedTurn) -> bool:
    """Renew the hook lease at a slice boundary; False stops the sweep.

    A renew that raises is a stop, not a crash: the caller's deadline
    escalation then closes the run ``failed``.
    """
    carry = constants._HOOK_RUN_CARRY.get()
    if carry is None or carry.recorder is None or carry.ref is None:
        return False
    try:
        return await carry.recorder.renew(carry.ref, self._config.effective_hook_claim_lease_s)
    except HookRunRecorderError as exc:
        logger.warning(
            "sweep slice %s could not renew its hook lease (%s); not continuing",
            qevent.event_id,
            exc.code,
        )
        return False


async def _coverage_notice(
    self: Kernel, qevent: QueuedTurn, hook_outcome: HookRunOutcome
) -> str | None:
    """The coverage notice a stopped sweep owes, or None. Computes, never posts."""
    try:
        if (
            self._sweep is None
            or qevent.source is not TurnSource.CRON
            or routing._is_targetless(qevent)
            or hook_outcome not in _NOTICE_OUTCOMES
        ):
            return None
        read = await self._sweep_read(qevent)
        if parse_continuation(qevent.event_id) is None and read.checkpoint is None:
            # Not identifiable as a sweep: an ordinary hook failure is reported
            # by its escalation alone.
            return None
        carry = constants._HOOK_RUN_CARRY.get()
        state = carry.state if carry is not None else None
        hook = state.name if state is not None else qevent.author.removeprefix("cron:")
        return coverage_notice_text(hook=hook, outcome=hook_outcome, read=read)
    except Exception as exc:  # noqa: BLE001 - a notice never fails a turn
        logger.warning(
            "sweep coverage notice could not be computed for event %s: %s",
            qevent.event_id,
            type(exc).__name__,
        )
        return None


async def _post_coverage_notice(
    self: Kernel, qevent: QueuedTurn, route: TargetRoute, text: str
) -> None:
    """Post the notice as a NEW message to the hook's target. Never raises."""
    # Never the turn's minted ref: the notice is its own message, and it is
    # not the turn's result, so ``_reply_for`` (which adopts a ref and marks
    # the terminal reply) is deliberately not used.
    target = routing._target_for(qevent).model_copy(update={"reply_ref": None})
    try:
        await self._reply(target, route, text)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - best effort, after the settle was won
        logger.warning(
            "sweep coverage notice for event %s could not be delivered",
            qevent.event_id,
            exc_info=True,
        )
