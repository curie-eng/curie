from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

import aiohttp
from aci_protocol import (
    QueuedTurn,
    SessionStatus,
)
from channel_protocol.reply import (
    ReplyAck,
)

from ..delivery_lease import DeliveryLease
from ..sandbox.types import (
    PressureCandidate,
    QuotaRejection,
    RouteRecord,
    RouteState,
)
from ..threadlock import LockAcquireTimeout, LockLeaseLost

if TYPE_CHECKING:
    from .core import Kernel

from . import clock, constants, log, routing
from .log import logger


@dataclass(frozen=True)
class _PressureResult:
    reclaimed: bool
    outcome: str


async def notify_capacity_queued(self: Kernel, qevent: QueuedTurn) -> ReplyAck:
    # A queued edit that starts after the answer must not replace it. An
    # edit already inside the sink is a different window: the notice lock
    # holds the wake until that send finishes. An unreadable terminality
    # check raises so the notice loop reschedules instead of recording the
    # notice as delivered.
    if qevent.event_id in self._terminal_reply_attempted:
        return ReplyAck()
    try:
        terminal = await self._markers.is_terminal(qevent.event_id)
    except asyncio.CancelledError:
        raise
    if terminal:
        return ReplyAck()
    try:
        return await self._reply_for(
            qevent,
            routing._route_from_handle(qevent),
            "The agent is busy. Your request is queued and will start when space opens.",
            terminal=False,
        )
    finally:
        self._minted_refs.pop(qevent.event_id, None)


async def expire_capacity_wait(
    self: Kernel,
    qevent: QueuedTurn,
    *,
    lease: DeliveryLease,
    cause: str,
    grant_epoch: str | None,
) -> tuple[bool, str | None]:
    lease.raise_if_lost()
    # A wait without a recorded grant never started work. A later message
    # may now own this thread, so only stop the exact epoch of this wait.
    if grant_epoch is not None:
        await self._quiesce_capacity_epoch(routing._thread_key_for(qevent), grant_epoch)
    lease.raise_if_lost()
    route = routing._route_from_handle(qevent)
    delivered = False
    reply_ref: str | None = None
    try:
        try:
            text = (
                constants._CAPACITY_FAILED_REPLY
                if cause == "delivery_exhausted"
                else constants._CAPACITY_UNKNOWN_REPLY
                if cause == "grant_unknown"
                else constants._CAPACITY_EXPIRED_REPLY
            )
            ack = await self._reply_for(qevent, route, text)
            reply_ref = ack.ref
            delivered = True
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
            logger.warning(
                "capacity wait expiry reply failed for event %s",
                qevent.event_id,
                exc_info=True,
            )
        await self._complete(
            qevent,
            route,
            "escalated",
            telemetry_outcome="capacity_wait_expired",
            lease=lease,
        )
        return delivered, reply_ref
    finally:
        self._minted_refs.pop(qevent.event_id, None)
        self._terminal_reply_attempted.discard(qevent.event_id)


async def resolve_capacity_grant(self: Kernel, qevent: QueuedTurn, epoch: str) -> str:
    """Attest and stop an uncertain grant before terminal classification."""

    return await self._quiesce_capacity_epoch(routing._thread_key_for(qevent), epoch)


async def notify_capacity_expired(self: Kernel, qevent: QueuedTurn, *, cause: str) -> ReplyAck:
    try:
        text = (
            constants._CAPACITY_FAILED_REPLY
            if cause == "delivery_exhausted"
            else constants._CAPACITY_UNKNOWN_REPLY
            if cause == "grant_unknown"
            else constants._CAPACITY_EXPIRED_REPLY
        )
        return await self._reply_for(qevent, routing._route_from_handle(qevent), text)
    finally:
        self._minted_refs.pop(qevent.event_id, None)
        self._terminal_reply_attempted.discard(qevent.event_id)


def _record_pressure_outcome(outcome: str) -> None:
    """Emit one bounded, identifier free result for a pressure attempt."""

    if outcome not in constants._PRESSURE_OUTCOMES:
        outcome = "timeout"
    log.record_metric(
        "curie.sandbox.lifecycle",
        attributes={
            "service.name": "curie-worker",
            "operation": "reclaim",
            "outcome": outcome,
        },
    )


def _pressure_record_is_safe(
    self: Kernel, candidate: PressureCandidate, record: RouteRecord
) -> bool:
    """Fail closed unless the persisted route itself permits reclamation."""

    handle = record.handle
    return (
        record.state is RouteState.LIVE
        and handle.thread_key == candidate.thread_key
        and handle.namespace == self._substrate.namespace
        and isinstance(handle.history_ref, str)
        and bool(handle.history_ref.strip())
        and bool(handle.token)
        and handle.workspace_repo is None
        and handle.workspace_materialized_head is None
        and handle.publication_visible_outcome_revision == 0
    )


def _pressure_status_is_safe(status: dict[str, object]) -> bool:
    """Accept only an inactive durable runner in a completed idle state."""

    return (
        status.get("turn_active") is False
        and status.get("history_durable") is True
        and status.get("status")
        in {
            SessionStatus.DONE.value,
            SessionStatus.IDLE_AWAITING_INPUT.value,
        }
    )


async def _reclaim_idle_route(
    self: Kernel,
    requesting_thread_key: str,
    rejection: QuotaRejection,
    *,
    remaining_s: float,
) -> _PressureResult:
    """Detach and delete at most one proven safe idle route."""

    pressure_deadline = clock.time.monotonic() + min(remaining_s, constants._PRESSURE_CEILING_S)
    try:
        async with asyncio.timeout_at(pressure_deadline):
            return await self._reclaim_idle_route_before_deadline(
                requesting_thread_key,
                rejection,
                pressure_deadline=pressure_deadline,
            )
    except TimeoutError:
        return _PressureResult(False, "timeout")


async def _reclaim_idle_route_before_deadline(
    self: Kernel,
    requesting_thread_key: str,
    rejection: QuotaRejection,
    *,
    pressure_deadline: float,
) -> _PressureResult:
    """Run one pressure pass under the caller's hard async deadline."""

    inventory_deadline = min(
        pressure_deadline,
        clock.time.monotonic() + constants._PRESSURE_SCAN_DEADLINE_S,
    )
    try:
        async with asyncio.timeout_at(inventory_deadline):
            inventory = await self._substrate.pressure_candidates(
                max_pages=constants._PRESSURE_SCAN_PAGES,
                max_records=constants._PRESSURE_SCAN_RECORDS,
                deadline=inventory_deadline,
            )
    except Exception as exc:  # noqa: BLE001 - pressure inventory fails closed
        logger.warning(
            "idle route reclamation inventory failed: %s",
            type(exc).__name__,
        )
        return _PressureResult(False, "timeout")

    if inventory.outcome != "complete":
        return _PressureResult(False, inventory.outcome)

    saw_race = False
    # Idle eval routes go first, so a liveness probe's sandboxes are freed
    # before a person's. The sort is stable: each group keeps the
    # inventory's (expiry, thread key) order.
    candidates = tuple(
        sorted(
            (
                candidate
                for candidate in inventory.candidates
                if candidate.thread_key != requesting_thread_key
            ),
            key=lambda candidate: not routing._is_eval_thread_key(candidate.thread_key),
        )
    )[: constants._PRESSURE_CANDIDATES]
    for candidate in candidates:
        if clock.time.monotonic() >= pressure_deadline:
            return _PressureResult(False, "timeout")
        candidate_deadline = min(
            pressure_deadline,
            clock.time.monotonic() + constants._PRESSURE_CANDIDATE_CEILING_S,
        )
        detached: RouteRecord | None = None
        detach_result_unknown = False
        try:
            async with asyncio.timeout_at(candidate_deadline):
                async with self._pressure_lock.hold(
                    self._config.lock_key(candidate.thread_key)
                ) as lease:
                    record = await self._substrate.pressure_get(candidate.thread_key)
                    if record is None or not self._pressure_record_is_safe(candidate, record):
                        continue
                    status_remaining = min(
                        constants._PRESSURE_RUNNER_STATUS_S,
                        candidate_deadline - clock.time.monotonic(),
                    )
                    if status_remaining <= 0:
                        raise TimeoutError
                    try:
                        status = await self._runner.status(
                            record.handle.base_url,
                            token=record.handle.token,
                            remaining_s=status_remaining,
                        )
                    except (TimeoutError, aiohttp.ClientError) as exc:
                        logger.warning(
                            "idle route reclamation skipped an unreadable runner status: %s",
                            type(exc).__name__,
                        )
                        continue
                    if not self._pressure_status_is_safe(status):
                        continue
                    try:
                        detached_now = await self._substrate.detach_if_unchanged(
                            candidate.thread_key,
                            expected_claim=record.handle.claim_name,
                            expected_generation=record.handle.generation,
                            expected_expires_at_ms=candidate.expires_at_ms,
                            lock_key=lease.key,
                            lock_token=lease.token,
                        )
                    except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
                        detach_result_unknown = True
                        raise
                    if not detached_now:
                        saw_race = True
                        continue
                    detached = record
        except (LockAcquireTimeout, LockLeaseLost):
            if detach_result_unknown:
                return _PressureResult(False, "timeout")
            saw_race = True
        except TimeoutError:
            return _PressureResult(False, "timeout")
        except Exception as exc:  # noqa: BLE001 - an unreadable candidate is unsafe
            if detach_result_unknown:
                logger.warning(
                    "idle route reclamation detach result was unknown: %s",
                    type(exc).__name__,
                )
                return _PressureResult(False, "timeout")
            logger.warning(
                "idle route reclamation skipped an unreadable candidate: %s",
                type(exc).__name__,
            )

        if detached is None:
            continue
        cleanup_deadline = min(
            pressure_deadline,
            clock.time.monotonic() + constants._PRESSURE_CLEANUP_CEILING_S,
        )
        try:
            async with asyncio.timeout_at(cleanup_deadline):
                deleted = await asyncio.to_thread(
                    self._substrate.delete_detached,
                    detached,
                    rejection,
                    deadline=cleanup_deadline,
                )
        except TimeoutError:
            return _PressureResult(False, "timeout")
        return _PressureResult(deleted, "reclaimed" if deleted else "timeout")

    if saw_race:
        return _PressureResult(False, "race-lost")
    return _PressureResult(False, "refused-no-safe-route")
