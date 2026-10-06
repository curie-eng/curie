from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from aci_protocol import (
    QueuedTurn,
)

from ..delivery_lease import DeliveryLease
from ..killswitch import KillSwitch
from ..workitem_dispatch import (
    TerminationObservation,
    WorkItemAcquireGrant,
    WorkItemConflict,
    WorkItemRun,
    WorkItemTransportError,
    parse_work_item_event_id,
)

if TYPE_CHECKING:
    from .core import Kernel

from . import clock, constants, failures, memory, routing
from .log import logger


def owns_work_item(self: Kernel, request_id: uuid.UUID) -> bool:
    """Whether this process still holds the WorkItem run (#3076).

    A live run that has not finished is ours, and the orphan sweeper must
    never declare it lost. A run parked for approval is ours only until its
    execution deadline: past it the continuation can no longer finish the
    request, so the entry is evicted and the sweeper may declare it lost
    (#3564).
    """

    self._evict_expired_held_work_items()
    run = self._work_item_runs.get(request_id)
    if run is not None and not run.finished:
        return True
    return any(held.request_id == request_id for held in self._held_work_items.values())


def _evict_expired_held_work_items(self: Kernel) -> None:
    """Drop held runs whose execution deadline has passed (#3564).

    A held run already closed its heartbeat when it parked, so eviction is
    local bookkeeping only. A held run always has a deadline, because
    holding requires start; an entry without one is kept.
    """

    now = datetime.now(UTC)
    for thread_key, held in list(self._held_work_items.items()):
        if held.execution_deadline is None or held.execution_deadline > now:
            continue
        del self._held_work_items[thread_key]
        logger.warning(
            "evicting held work item %s on thread %s: execution deadline %s passed",
            held.request_id,
            thread_key,
            held.execution_deadline.isoformat(),
        )


def _forget_held_work_items(
    self: Kernel,
    *,
    reason: str,
    thread_key: str | None = None,
    agent_id: uuid.UUID | None = None,
) -> None:
    """Drop held runs for a thread or an agent, so none outlives its end (#3564)."""

    for key, held in list(self._held_work_items.items()):
        if thread_key is not None and key != thread_key:
            continue
        if agent_id is not None and held.agent_id != agent_id:
            continue
        del self._held_work_items[key]
        logger.warning(
            "dropping held work item %s on thread %s: %s",
            held.request_id,
            key,
            reason,
        )


def _run_for_event(self: Kernel, event_id: str) -> WorkItemRun | None:
    """The execution bound to this turn, not whichever run started last."""

    for candidate in self._work_item_runs.values():
        if candidate.event_id == event_id and candidate.started and not candidate.finished:
            return candidate
    return None


def _is_factory_work_item_turn(self: Kernel, event_id: str) -> bool:
    """Whether this turn's requesting chat belongs to a factory execution.

    Execute wake ids are platform minted and fully parsed. Approval resumes
    have their own id namespace, so they count only after WorkItem ownership
    ties the exact event to a factory conversation. Message text and channel
    kind confer no factory identity.
    """

    parsed = parse_work_item_event_id(event_id)
    if parsed is not None:
        return parsed.kind in {"execute", "ci"}
    return event_id in self._factory_work_item_events or self._run_for_event(event_id) is not None


def _work_item_repository(self: Kernel, event_id: str) -> str | None:
    """The WorkItem repository for an acquired execute wake, else None."""

    parsed = parse_work_item_event_id(event_id)
    if parsed is None or parsed.kind != "execute":
        return None
    run = self._work_item_runs.get(parsed.request_id)
    return run.repository_path if run is not None else None


async def _adopt_resumed_work_item(
    self: Kernel, event_id: str, thread_key: str
) -> uuid.UUID | None:
    """Reattach the execution an approval continuation still owns.

    The execute wake already returned. The same process keeps the run.
    A restarted worker reads the still-running request by conversation.
    Either way the continuation finishes that request and bounds the
    runner by the original execution deadline.
    """

    # An expired held run cannot be finished by this continuation; evict it
    # so the lookup below asks the API instead (#3564).
    self._evict_expired_held_work_items()
    held = self._held_work_items.pop(thread_key, None)
    if held is None and self._work_items is not None:
        try:
            found = await self._work_items.running_for_conversation(thread_key)
        except WorkItemTransportError:
            raise
        except WorkItemConflict as exc:
            if exc.code == "execution_ended":
                raise failures._FactoryExecutionEnded from exc
            found = None
        if found is not None:
            held = WorkItemRun(
                client=self._work_items,
                request_id=found.request_id,
                owner=self._config.consumer_name,
                grant=WorkItemAcquireGrant(
                    generation=0,
                    work_item_id=found.work_item_id,
                    conversation_id=thread_key,
                    wait_deadline="",
                    repository_path=None,
                ),
                event_id=event_id,
                thread_key=thread_key,
                on_stop=self._stop_owned_work_item,
                on_stale=self._abandon_stale_work_item,
            )
            held.started = True
            held.runtime_epoch = found.runtime_epoch
            held.execution_deadline = found.execution_deadline
    if held is None:
        return None
    held.held = False
    held.finished = False
    held.event_id = event_id
    self._work_item_runs[held.request_id] = held
    self._factory_work_item_events.add(event_id)
    return held.request_id


async def reap_orphans(self: Kernel) -> list[str]:
    """Periodic tick: delete substrate claims no live route references."""
    reaped = await asyncio.to_thread(self._substrate.reap_orphans)
    if self._workspace is not None:
        expired_threads = await asyncio.to_thread(self._workspace.enumerate_expired)
        for thread_key in expired_threads:
            # Enumeration is advisory. Serialize with claim/touch/release,
            # then re-read this exact ledger so an active base staged by a
            # different worker after enumeration cannot be deleted.
            async with self._lock.hold(self._config.lock_key(thread_key)) as lease:
                candidate = await asyncio.to_thread(
                    self._workspace.begin_expired_reap,
                    thread_key,
                )
                if candidate is not None:
                    # The object deletes above can be slow. Fence the final
                    # ledger delete with the still-current Valkey token; the
                    # exact-ledger comparison is the second guard if a lease
                    # is lost immediately after this check.
                    await lease.ensure_owned()
                    await asyncio.to_thread(
                        self._workspace.finish_expired_reap,
                        candidate,
                    )
    if self._attachments is not None:
        # The SIBLING sweep, in this same tick and behind this same
        # per-thread fence rather than arriving with a scheduler of its own.
        # The lanes stay separate because the ledgers are: the workspace one
        # is thread-ownership on the route lease, this one is "may a retry
        # still fetch these bytes" on the retention window.
        for parked_thread in await asyncio.to_thread(self._attachments.enumerate_expired):
            async with self._lock.hold(self._config.lock_key(parked_thread)) as lease:
                parked = await asyncio.to_thread(
                    self._attachments.begin_expired_reap,
                    parked_thread,
                )
                if parked is not None:
                    # Same reason as above: the object deletes can outlive
                    # the original lease, so the final ledger delete is
                    # fenced with a still-current token and guarded a second
                    # time by the exact-record comparison inside `finish`.
                    await lease.ensure_owned()
                    await asyncio.to_thread(
                        self._attachments.finish_expired_reap,
                        parked,
                    )
    return reaped


async def _preflight_reclaimed_delivery(
    self: Kernel, thread_key: str, lease: DeliveryLease
) -> None:
    """Prove a TRANSFERRED delivery is safe to re-execute, or refuse it.

    ADR-0131's reclaim rules 2, 3 and 4, in that order. Rule 1 -- "a
    side-effect marker forbids replay and settles to human escalation" -- is
    the existing ``saw_side_effect`` check in ``_process_event``, which runs
    BEFORE this and needs no code here; that ordering is load-bearing and is
    pinned by a test rather than duplicated as a second check, because a
    preflight that interrupted, waited and then rehydrated could otherwise
    re-execute a half-done non-idempotent action.

    Rule 2: a runner that still reports an active turn is interrupted and
    must become idle (or disappear) before the retry. The interrupt goes
    through ``interrupt_thread``, the EXISTING bounded control path -- no
    second mechanism, and never a bare task cancel, which would skip the
    runner-side stop.

    Rule 3: an unreadable runner FAILS CLOSED. ``_turn_active`` already
    reports busy when liveness cannot be read, so an unreadable runner
    exhausts the bounded poll and this raises, leaving the stream entry
    pending. A replacement never runs beside a possibly-active turn -- the
    "just retry it" simplification is precisely the failure mode this
    refuses.

    Rule 4 is what falling off the end of this method means: old authority is
    fenced (the lease generation moved), the old runner is inactive, and no
    side-effect marker exists.

    This deliberately diverges from the ordinary route, which STEERS into a
    retained live turn: this is the same event being retried, not a follow-up.
    The choice is driven by the lease generation, an explicit distributed
    state fact, so kernel rule 3 ("never keyword-guess intent") is untouched.
    """
    # ONE deadline for the WHOLE preflight, established before the first
    # probe -- not merely the gap between polls. Every liveness probe below
    # derives its per-request HTTP bound from what is left of it, so a wedged
    # runner (accepts the connect, then answers nothing) cannot ride
    # ``RunnerClient``'s 600s streaming budget inside a single iteration. It
    # could otherwise burn the delivery's entire deadline here and the next
    # pass would escalate ``deadline_halted`` having made ZERO attempts,
    # which is the opposite of "delivery is bounded". Clamped to the
    # delivery's own remaining budget as well as the constant, so recovering
    # one transferred delivery can never eat the whole deadline.
    #
    # Exhausting the window is not a silent pass: a probe cut short by it
    # fails closed (``_turn_active`` reports busy on any unreadable answer)
    # and the loop falls through to the raise below, which is exactly rule
    # 3's refusal.
    deadline = clock.time.monotonic() + min(
        constants._RECLAIM_PREFLIGHT_IDLE_TIMEOUT_S, max(0.0, lease.remaining_s())
    )

    def _left() -> float:
        return max(0.0, deadline - clock.time.monotonic())

    handle = await asyncio.to_thread(self._substrate.lookup, thread_key)
    if handle is None:
        # No retained sandbox: the previous owner's runner is gone, which is
        # the "or disappear" half of rule 2.
        return
    if not await self._turn_active(handle, remaining_s=_left()):
        return
    logger.warning(
        "reclaimed delivery for thread %s (generation %d) found a live turn "
        "from a previous owner; interrupting before retry",
        thread_key,
        lease.generation,
    )
    # The interrupt is deliberately NOT given this window: it keeps its own
    # independent control-plane timeout, because a fence derived from a
    # budget that may already be spent could not stop the runner it fenced.
    await self.interrupt_thread(thread_key, "fenced transfer of a reclaimed delivery")
    while clock.time.monotonic() < deadline:
        await asyncio.sleep(min(constants._RECLAIM_PREFLIGHT_POLL_S, _left()))
        handle = await asyncio.to_thread(self._substrate.lookup, thread_key)
        if handle is None or not await self._turn_active(handle, remaining_s=_left()):
            return
    raise memory.ReclaimPreflightUnsafe(
        f"thread {thread_key} still reports (or cannot deny) a live turn after "
        "the reclaim interrupt; refusing to run a replacement beside it"
    )


async def _quiesce_capacity_epoch(self: Kernel, thread_key: str, epoch: str) -> str:
    """Stop only this epoch and return its attested admission decision."""

    deadline = clock.time.monotonic() + constants._RECLAIM_PREFLIGHT_IDLE_TIMEOUT_S
    handle = await asyncio.to_thread(self._substrate.lookup, thread_key)
    if handle is None:
        return "unknown"
    timeout_sent = False
    while clock.time.monotonic() < deadline:
        remaining_s = max(0.0, deadline - clock.time.monotonic())
        try:
            status = await self._runner.capacity_status(
                handle.base_url,
                epoch=epoch,
                token=handle.token or None,
                remaining_s=min(1.0, remaining_s),
            )
        except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
            raise memory.ReclaimPreflightUnsafe(
                f"capacity runner status unreadable for thread {thread_key}"
            ) from exc
        if (
            status.get("capacity_admission") is not True
            or "turn_epoch" not in status
            or status.get("capacity_admission_result")
            not in {"pending", "granted", "denied", "unknown"}
        ):
            raise memory.ReclaimPreflightUnsafe(
                f"capacity runner status invalid for thread {thread_key}"
            )
        if status["turn_epoch"] != epoch:
            return str(status["capacity_admission_result"])
        if not timeout_sent:
            await self._runner.timeout_turn(handle.base_url, epoch, token=handle.token or None)
            timeout_sent = True
        await asyncio.sleep(
            min(constants._RECLAIM_PREFLIGHT_POLL_S, max(0.0, deadline - clock.time.monotonic()))
        )
    raise memory.ReclaimPreflightUnsafe(
        f"capacity runner turn remains live for thread {thread_key}"
    )


async def _release_work_item_sandbox(self: Kernel, thread_key: str) -> None:
    """Delete a settled WorkItem thread's claim, live or suspended (#3075).

    Best-effort: a failure is logged and never changes the WorkItem outcome.
    The orphan reaper skips any claim a route references, so without this a
    suspended route kept its claim until someone deleted it by hand.
    """

    try:
        token = await asyncio.wait_for(
            self._lock.acquire(self._config.lock_key(thread_key)),
            constants._RESET_LOCK_ACQUIRE_TIMEOUT_S,
        )
    except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
        logger.warning(
            "work-item sandbox release could not lock thread %s; leaving the claim",
            thread_key,
            exc_info=True,
        )
        return
    try:
        released = await asyncio.wait_for(
            asyncio.to_thread(self._substrate.release, thread_key),
            constants._RESET_RELEASE_TIMEOUT_S,
        )
        if released and self._workspace is not None:
            await asyncio.to_thread(self._workspace.release, thread_key)
    except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
        logger.warning(
            "work-item sandbox release failed for thread %s",
            thread_key,
            exc_info=True,
        )
    finally:
        await self._lock.release(self._config.lock_key(thread_key), token)


async def _terminate_work_item(self: Kernel, qevent: QueuedTurn, request_id: uuid.UUID) -> None:
    """Claim termination ownership, observe sandbox absence, and record it."""

    if self._work_items is None:
        logger.error(
            "work-item terminate %s has no dispatch client; dropping the wake",
            qevent.event_id,
        )
        return
    thread_key = routing._thread_key_for(qevent)
    try:
        epoch = await self._work_items.claim_termination(
            request_id,
            owner=self._config.consumer_name,
        )
    except WorkItemConflict as exc:
        logger.info(
            "work-item terminate claim refused for %s: %s",
            request_id,
            exc.code,
        )
        return
    # Cleared only once the claim succeeds, so a refused claim leaves the
    # held run in place for whoever does own the termination (#3564).
    self._forget_held_work_items(thread_key=thread_key, reason="terminated")
    claim_name: str | None = None
    sandbox_name: str | None = None
    try:
        view = await self._work_items.get_request(request_id)
    except WorkItemConflict as exc:
        logger.info(
            "work-item terminate could not read request %s: %s",
            request_id,
            exc.code,
        )
    else:
        claim_name = view.runtime_claim_name
        sandbox_name = view.runtime_sandbox_name
    observation = await self._halt_work_item_runtime(
        thread_key,
        claim_name=claim_name,
        sandbox_name=sandbox_name,
    )
    if observation is None:
        logger.warning(
            "work-item terminate for %s did not observe absence; reconciler retries",
            request_id,
        )
        return
    try:
        await self._work_items.record_termination(
            request_id,
            runtime_epoch=epoch,
            observation=observation.render(),
        )
    except WorkItemConflict as exc:
        logger.warning(
            "work-item record_termination refused for %s: %s",
            request_id,
            exc.code,
        )


async def _stop_owned_work_item(self: Kernel, thread_key: str, run: WorkItemRun) -> None:
    """Heartbeat saw cancellation_requested: interrupt, observe, record."""

    self._forget_held_run(thread_key, run, reason="cancellation requested")
    if self._work_items is None or run.runtime_epoch is None:
        return
    observation = await self._halt_work_item_runtime(
        thread_key,
        claim_name=run.claim_name,
        sandbox_name=run.sandbox_name,
    )
    if observation is None:
        logger.warning(
            "work-item owner stop for %s did not observe absence",
            run.request_id,
        )
        return
    try:
        await self._work_items.record_termination(
            run.request_id,
            runtime_epoch=run.runtime_epoch,
            observation=observation.render(),
        )
        run.finished = True
    except WorkItemConflict as exc:
        logger.warning(
            "work-item owner record_termination refused for %s: %s",
            run.request_id,
            exc.code,
        )


async def _abandon_stale_work_item(self: Kernel, thread_key: str, run: WorkItemRun) -> None:
    """Heartbeat 409 stale_owner: drop local ownership without touching the current route."""

    run.finished = True
    self._forget_held_run(thread_key, run, reason="stale owner")
    logger.warning(
        "stale work-item owner abandoning request %s on thread %s without releasing the route",
        run.request_id,
        thread_key,
    )


def _forget_held_run(self: Kernel, thread_key: str, run: WorkItemRun, *, reason: str) -> None:
    """Drop the thread's held entry only when it is this very run (#3564)."""

    if self._held_work_items.get(thread_key) is run:
        self._forget_held_work_items(thread_key=thread_key, reason=reason)


async def _halt_work_item_runtime(
    self: Kernel,
    thread_key: str,
    *,
    claim_name: str | None,
    sandbox_name: str | None,
) -> TerminationObservation | None:
    """Interrupt, then poll until stored claim and sandbox names are gone."""

    try:
        await asyncio.wait_for(
            self.interrupt_thread(thread_key, "work-item termination"),
            constants._RESET_INTERRUPT_TIMEOUT_S,
        )
    except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
        logger.warning(
            "work-item interrupt did not land for thread %s; terminating anyway",
            thread_key,
            exc_info=True,
        )
    async with self._lock.hold(self._config.lock_key(thread_key)):
        return await asyncio.to_thread(
            self._substrate.terminate_thread,
            thread_key,
            claim_name=claim_name,
            sandbox_name=sandbox_name,
            observer=self._config.consumer_name,
        )


def attach_killswitch(self: Kernel, killswitch: KillSwitch) -> None:
    """Wire the kill switch after construction (it needs interrupt_agent)."""
    self._killswitch = killswitch


def _register_run(self: Kernel, agent_id: uuid.UUID | None, thread_key: str) -> None:
    # Keyed by the scoped thread key, because the kill fan-out hands each
    # entry straight to ``interrupt_thread`` -> ``substrate.lookup``.
    if agent_id is not None:
        self._active_by_agent.setdefault(agent_id, set()).add(thread_key)


def _unregister_run(self: Kernel, agent_id: uuid.UUID | None, thread_key: str) -> None:
    if agent_id is None:
        return
    threads = self._active_by_agent.get(agent_id)
    if threads is not None:
        threads.discard(thread_key)
        if not threads:
            del self._active_by_agent[agent_id]
