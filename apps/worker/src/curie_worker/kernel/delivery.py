from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from aci_protocol import (
    QueuedTurn,
    SessionStatus,
)
from channel_protocol.reply import (
    REPLY_WIRE_VERSION,
    NavAffordance,
    ReplyTarget,
    ReplyUpdate,
)

from ..delivery_lease import DeliveryLease, LeaseLostError
from ..receipt import TurnReceiptMode, render_receipt
from ..reply_sink import (
    ObservedReplySink,
    ReplySink,
    TargetRoute,
)

if TYPE_CHECKING:
    from .core import Kernel

from . import clock, constants, routing, workspace
from .log import logger


def _join_reply_blocks(*parts: str | None) -> str:
    """Join reply blocks with one blank line, skipping empty or absent ones.

    The one place reply blocks are composed, so the CLI's blank-line block parse
    (``parse_approval_id`` splits on ``\\n\\n``) always sees whole blocks and never
    an empty one.
    """

    return "\n\n".join(part for part in parts if part)


@dataclass
class _LockEntry:
    """A per-thread in-process lock plus a holder/waiter refcount so the entry
    can be evicted when idle (otherwise the map grows one entry per thread ever
    seen, an unbounded leak in a long-running worker)."""

    lock: asyncio.Lock
    refs: int = 0


@dataclass
class _StreamAccumulator:
    text_parts: list[str] = field(default_factory=list)
    saw_side_effect: bool = False
    classification: str | None = None
    error_message: str | None = None
    status: SessionStatus | None = None
    final_text: str | None = None
    tools_called: set[str] = field(default_factory=set)
    approval_summary: str | None = None
    approval_route: str | None = None
    approval_gate_kind: str | None = None
    approval_granted_tool: str | None = None
    approval_granted_arguments: dict[str, Any] | None = None
    approval_display: str | None = None
    # Call id -> the ledger record it opened. One CALL produces two ACI frames
    # (ADR-0117): the first opens a record, the second closes THAT record rather
    # than minting a second. A turn that calls the same tool twice is only
    # distinguishable here by the call id.
    open_actions: dict[str, str] = field(default_factory=dict)
    # Completed ledger rows, in the order their calls closed -- the receipt
    # (ADR-0117 decision 7). Read back from the ledger rather than assembled
    # here, because ``undoable`` is derived on the record and a receipt built
    # from what the worker SENT could claim a reversibility the row lacks.
    receipt_rows: list[dict[str, Any]] = field(default_factory=list)
    # The install's receipt mode (ADR-0180). Read only by rendered_with_receipt:
    # the ledger rows above and saw_side_effect are the same in every mode.
    receipt_mode: TurnReceiptMode = "all"
    # The repository this turn attached from its own message (#2659), announced
    # at finalize only. Intermediate streaming edits show `rendered()` alone.
    workspace_inferred_repo: str | None = None

    def rendered(self) -> str:
        return self.final_text if self.final_text is not None else "".join(self.text_parts)

    def rendered_with_receipt(self) -> str:
        """The turn's answer, then the inferred repository, then what it did.

        Appended rather than replacing: the model's answer is what the person
        asked for, and the receipt is the platform's own account beneath it. A
        turn that changed nothing adds nothing, because most turns are reads and
        a receipt on every one of them is noise.

        The inferred repository announcement (#2659) is a trailing block for the
        same reason the receipt is: the final edit appends beneath a message the
        person may already be reading, instead of rewriting its first line. It
        sits directly after the answer so the receipt stays the last block.
        """

        return _join_reply_blocks(
            self.rendered(),
            workspace._workspace_inference_notice(self.workspace_inferred_repo),
            render_receipt(self.receipt_rows, mode=self.receipt_mode),
        )


class _ThrottledReply:
    """Coalesces chat.update edits while streaming; always flushes the final. In
    no-edit mode intermediate edits are suppressed entirely, so the placeholder
    gets exactly one update (the final)."""

    def __init__(
        self,
        sink: ReplySink,
        *,
        target: ReplyTarget | None,
        route: TargetRoute,
        min_interval_s: float,
        nav: NavAffordance | None = None,
        no_edit: bool = False,
        best_effort: bool = False,
        on_ref: Callable[[str], None] | None = None,
        on_final: Callable[[], None] | None = None,
    ) -> None:
        self._sink = ObservedReplySink(sink)
        self._target = target
        # Told when this turn mints a reply ref (ADR-0079), so the kernel's other
        # delivery paths edit the same message this streamer created. Only fires
        # on a placeholder-less turn, where the first emit posts rather than edits.
        self._on_ref = on_ref
        # Told when the FINAL flush is attempted (#2433), mirroring ``on_ref``.
        # ``stream`` and ``context`` deliberately never call it: a streamed
        # fragment is a preview, not the answer, and one of #2433's three named
        # shapes is the turn that streamed partial text and then raised.
        self._on_final = on_final
        self._min_interval_s = min_interval_s
        self._no_edit = no_edit
        self._last = 0.0
        self._last_context: float | None = None
        self._last_text: str | None = None
        # Whether this turn's reply delivery is best-effort (#708): set only for an
        # approval-resume turn (the caller derives it from _is_approval_resume). The
        # granted tool already ran in the runner, so an undeliverable reply to a
        # now-dead CLI stub with no default transport completes the turn rather than
        # dead-lettering the resolved approval. Threaded to the sink per update.
        self._best_effort = best_effort
        # The bound agent's hub-button pack, forwarded to the sink so a render of
        # a COMPLETE structured reply can add the no-dead-ends hub button (in
        # practice the final flush, which is the update that carries one). None
        # when unbound/disabled.
        self._nav = nav
        # This turn's reply route (issue #19, ADR-0096 D4): which adapter
        # endpoint the edit is delivered to, and under whose credential. A kwarg
        # on ``emit``, never a wire field.
        self._route = route

    async def stream(self, text: str) -> None:
        if self._no_edit:
            return
        if not text or text == self._last_text:
            return
        now = clock.time.monotonic()
        if now - self._last < self._min_interval_s:
            return
        self._last = now
        self._last_text = text
        await self._emit(text)

    async def context(self, text: str) -> None:
        if self._no_edit:
            return
        if not text or text == self._last_text:
            return
        now = clock.time.monotonic()
        # The first context preview is separately eligible even after text;
        # later previews share the sustained write cadence with text updates.
        if (
            self._last_context is not None
            and now - max(self._last, self._last_context) < self._min_interval_s
        ):
            return
        # Stamp before emitting so a failed edit cannot create a hot retry loop.
        self._last_context = now
        self._last = now
        # A message creating emit must stay fail loud so ref minting and turn
        # retry semantics remain intact. Only an existing message edit is soft.
        if self._target is None or self._target.reply_ref is None:
            await self._emit(text)
            self._last_text = text
            return
        try:
            await self._emit(text)
            # Delivery state advances only after the edit succeeds, so a failed
            # preview cannot suppress an identical final flush.
            self._last_text = text
        except Exception as exc:  # noqa: BLE001 - cosmetic progress is best effort
            logger.warning("context reply update failed: %s", type(exc).__name__)

    async def finalize(self, text: str) -> None:
        # First statement, BEFORE the early return: an identical final means the
        # last streamed edit already IS the answer, so it is delivered and must
        # mark. Before the emit for the same reason ``_reply_for`` marks first.
        if self._on_final is not None:
            self._on_final()
        if text == self._last_text:
            return
        self._last_text = text
        await self._emit(text or "(no response)")

    async def _emit(self, text: str) -> None:
        if self._target is None:
            # A targetless turn (#2963) or factory execution (#2991): the stream
            # is consumed, never sent to the requesting chat.
            return
        ack = await self._sink.emit(
            ReplyUpdate(
                version=REPLY_WIRE_VERSION,
                event="reply.update",
                target=self._target,
                text=text,
                nav=self._nav,
            ),
            route=self._route,
            best_effort_unreachable=self._best_effort,
        )
        # A placeholder-less turn's first emit CREATES its message; adopt the ref
        # so every later delta edits that message instead of posting another one.
        # Without this a streamed job would post one message per throttle window.
        if self._target.reply_ref is None and ack.ref:
            self._target = self._target.model_copy(update={"reply_ref": ack.ref})
            if self._on_ref is not None:
                self._on_ref(ack.ref)


async def notify_turn_not_started(
    self: Kernel, qevent: QueuedTurn, *, lease: DeliveryLease | None = None
) -> None:
    """Tell the person their turn failed and is waiting for its retry (#2433).

    Called from the consumer's ``except`` branch, where a delivery whose
    handler raised is left pending. The log there is the operator's surface
    and it always worked; the person on the other end of the thread had none,
    and sat on the dispatcher's placeholder until the delivery was
    redelivered.

    It fires REGARDLESS of ``slack_no_edit_streaming``. That flag's promise is
    "the placeholder gets exactly one chat.update: the final". A turn that
    never finished emits no final, so obeying it here would leave the
    placeholder frozen, the reported bug reproduced inside its fix.

    It is best-effort by construction: a Slack outage may not turn a pending
    delivery into a failed one, so nothing here raises. ``CancelledError``
    still propagates, because cooperative shutdown is not a notice failure.

    A placeholder-less turn (CLI, job, hook) is skipped entirely: with nothing
    to edit, ``_reply_for`` POSTS a new message and adopts the minted ref,
    which would hand every such failure a spurious message it never had.

    Args:
        qevent: The queued turn whose delivery failed.
        lease: This delivery's ownership fence, when the caller holds one.
    """
    event_id = qevent.event_id

    # Popped FIRST so the entry cannot leak if a later guard returns early.
    # This is the in-process half of the delivered-answer guard: some
    # person-facing RESULT was already sent during this delivery, and the
    # notice must not overwrite it with an invitation to resend.
    attempted = event_id in self._terminal_reply_attempted
    self._terminal_reply_attempted.discard(event_id)
    factory_work_item_turn = self._is_factory_work_item_turn(event_id)
    self._factory_work_item_events.discard(event_id)

    if factory_work_item_turn:
        logger.debug(
            "event %s belongs to a factory execution; no not started notice",
            event_id,
        )
        return

    handle = qevent.reply_handle
    if handle is None:
        logger.debug(
            "event %s has no reply target; no not started notice",
            event_id,
        )
        return

    if handle.placeholder is None:
        logger.debug(
            "event %s has no placeholder to edit; no not-started notice",
            event_id,
        )
        return

    if attempted:
        logger.warning(
            "skipping the not-started notice for event %s: this delivery had "
            "already sent the person a result, and an ambiguous send may still "
            "have landed",
            event_id,
        )
        return

    try:
        # The durable half. ``is_terminal``, not a bare done marker: a DONE
        # outbox record proves the turn finished just as well and outlives the
        # marker. ``_complete`` emits the reply BEFORE it settles, so a settle
        # that applies in Valkey and then loses its response unwinds into the
        # consumer's except branch with the answer already on the person's
        # screen and the done marker already written.
        already_terminal = await self._markers.is_terminal(event_id)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
        # FAIL CLOSED. The two errors are not symmetric: a notice skipped for
        # a turn that did fail costs the person the seconds until the
        # lease-expiry reclaim redelivers, which is the behaviour before this
        # change; a notice sent for a turn that succeeded costs them the
        # answer permanently.
        logger.warning(
            "terminality unreadable for event %s; skipping the not-started "
            "notice rather than risk overwriting a delivered answer",
            event_id,
            exc_info=True,
        )
        return

    if already_terminal:
        logger.warning(
            "event %s failed after settling terminally; leaving its delivered "
            "reply in place rather than promising a retry that cannot happen",
            event_id,
        )
        return

    if lease is not None:
        try:
            # Re-checked at the emission boundary, AFTER the terminality read:
            # a heartbeat can mark the lease lost while that read is pending,
            # so a check taken only at the call site is already stale by the
            # time the edit happens. An in-process event read, no Valkey call:
            # ADR-0131 fences four verbs (ACK, dead-letter, clear record,
            # terminal result) and this non-terminal edit is none of them, and
            # a stale notice is self-correcting because the replacement's own
            # booting edit overwrites it within milliseconds of its acquire.
            lease.raise_if_lost()
        except LeaseLostError:
            logger.warning(
                "skipping the not-started notice for event %s: this owner lost "
                "the delivery lease, and the current owner speaks for the "
                "thread",
                event_id,
            )
            return

    try:
        await self._reply_for(
            qevent,
            routing._route_from_handle(qevent),
            self._config.turn_not_started_text,
            terminal=False,
        )
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
        logger.warning(
            "the not-started notice for event %s could not be delivered",
            event_id,
            exc_info=True,
        )


async def notify_broker_entry_vanished(
    self: Kernel, qevent: QueuedTurn, *, lease: DeliveryLease | None
) -> None:
    """Edit the placeholder after the broker entry is gone.

    Unlike :meth:`notify_turn_not_started`, lease loss is not a reason to
    stay silent. ADR-0131 skips the notice when a successor owns the fence.
    A vanished entry has no successor, so this edit is allowed only when
    ``lease.entry_vanished`` is set. It is still best-effort and it still
    refuses to overwrite a terminal reply.
    """
    if lease is None or not lease.entry_vanished.is_set():
        return

    event_id = qevent.event_id
    attempted = event_id in self._terminal_reply_attempted
    self._terminal_reply_attempted.discard(event_id)
    factory_work_item_turn = self._is_factory_work_item_turn(event_id)
    self._factory_work_item_events.discard(event_id)

    if factory_work_item_turn:
        logger.debug(
            "event %s belongs to a factory execution; no not started notice",
            event_id,
        )
        return

    handle = qevent.reply_handle
    if handle is None:
        logger.debug(
            "event %s has no reply target; no not started notice",
            event_id,
        )
        return

    if handle.placeholder is None:
        logger.debug(
            "event %s has no placeholder to edit; no not-started notice",
            event_id,
        )
        return

    if attempted:
        logger.warning(
            "skipping the not-started notice for event %s: this delivery had "
            "already sent the person a result, and an ambiguous send may still "
            "have landed",
            event_id,
        )
        return

    try:
        already_terminal = await self._markers.is_terminal(event_id)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
        logger.warning(
            "terminality unreadable for event %s; skipping the not-started "
            "notice rather than risk overwriting a delivered answer",
            event_id,
            exc_info=True,
        )
        return

    if already_terminal:
        logger.warning(
            "event %s failed after settling terminally; leaving its delivered "
            "reply in place rather than promising a retry that cannot happen",
            event_id,
        )
        return

    try:
        await self._reply_for(
            qevent,
            routing._route_from_handle(qevent),
            self._config.turn_not_started_text,
            terminal=False,
        )
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
        logger.warning(
            "the not-started notice for event %s could not be delivered",
            event_id,
            exc_info=True,
        )


async def interrupt_thread(self: Kernel, thread_key: str, reason: str) -> bool:
    """Hard-stop the thread's live turn. True if a live runner was signalled."""
    handle = await asyncio.to_thread(self._substrate.lookup, thread_key)
    if handle is None:
        return False
    await self._runner.interrupt(handle.base_url, reason, token=handle.token or None)
    return True


async def release_thread(self: Kernel, thread_key: str) -> bool:
    """Force-release the thread's sandbox (#713, an operator action): delete
    its substrate claim and route so the next message cold-creates fresh,
    picking up current model/Slack config instead of adopting a sandbox
    that may be running stale env from when it first booted. History is
    not lost -- a cold-created sandbox rehydrates its transcript from the
    durable state store on claim, the same as any other fresh claim.

    Any live turn is interrupted first so `release` never yanks the claim
    out from under a running turn without at least trying to stop it
    cleanly, but the interrupt is a courtesy and never a precondition
    (#739). The case that matters is a live handle whose runner is
    unresponsive: a wedged runner accepts the TCP connect and then answers
    `/v1/interrupt` never, so the call rides `RunnerClient`'s own 600s
    request timeout. That is exactly the sandbox an operator is resetting,
    and awaiting it unbounded costs twice: the release line below is never
    reached, and the reset request has already been SPOPped off the pending
    set by the maintenance tick, so a raise loses it permanently rather
    than retrying next tick. The same tick also owns stream reclaim and
    orphan reaping, which stall for the whole window behind it.

    So the interrupt is bounded to `_RESET_INTERRUPT_TIMEOUT_S` and any
    failure (timeout, transport error, non-200) is logged and swallowed; the
    release then runs unconditionally. `CancelledError` is deliberately not
    swallowed, so worker shutdown still cuts through.

    The release itself is also bounded, to `_RESET_RELEASE_TIMEOUT_S` (#743):
    a hang in the K8s control plane rather than the runner would otherwise
    stall this `asyncio.to_thread` -- and therefore the whole maintenance
    tick behind it -- indefinitely too, since `to_thread` is not
    cancellable. A timed-out release is NOT swallowed here; it propagates
    like any other release failure, which the drain loop's per-request
    handler already logs and isolates from the rest of the batch.

    The release runs under the SAME per-thread route lock the turn path
    holds around `_route_and_start` (#734). Without it the release is
    lock-free while a concurrent turn for this thread holds only that lock,
    so a message arriving in the window between the interrupt above and the
    release below can `claim()`-adopt the very sandbox this is tearing down
    and open a turn on it -- which the release then yanks mid-run. The
    interrupt cannot cover that turn: it fired before the turn existed, so
    it no-oped. Taking the lock makes the two mutually exclusive. A reset
    that wins the lock drops the route while holding it, so a turn waiting
    to route then cold-creates a fresh sandbox (exactly the reset's intent)
    instead of adopting the doomed one. A turn that wins the lock opens
    first and the reset serializes behind its `_route_and_start`, then tears
    it down as an ordinary live-thread reset (the turn replays on a fresh
    sandbox via the `runner-error` retry path described below). Either way
    no turn is ever left streaming on a sandbox this released out from under
    it without the reset first having serialized against its start.

    The lock hold spans only the release (interrupt-then-lock, not the
    reverse), so the bounded-but-possibly-slow courtesy interrupt does not
    extend the window turn-starts for this thread are blocked; the hold is
    the release bound (a few seconds), well under the lock's TTL.

    The failure log is an ERROR, not a warning: it is the only record that a
    sandbox was pulled out from under a turn that may still be running, and
    there is no retry that would produce a second, louder signal. The two
    success shapes are logged apart from it (and from each other) so an
    operator can tell "the live turn was killed" from "nothing was running"
    from "we released blind".

    Behavioral note for the unconditional release: when the thread really did
    have a live turn, tearing its sandbox down mid-run drops the turn stream,
    which `_consume` classifies as `runner-error`. That is in
    `RETRYABLE_CLASSIFICATIONS`, so the driving loop in `_run_event` retries
    the turn, and the retry re-claims, which cold-creates a fresh sandbox on
    current config -- exactly the state the reset was asking for. The
    no-auto-retry-after-side-effects rule still holds: an attempt that saw a
    `SideEffectFlag` escalates to a human instead of replaying. So a reset of
    a live thread is a replay on a fresh sandbox, not a lost turn, and the
    thread is never left routeless.

    True if a route existed to release."""
    # An operator release ends any run parked on the thread (#3564).
    self._forget_held_work_items(thread_key=thread_key, reason="operator release")
    await self._cancel_settling_work_items(thread_key=thread_key)
    try:
        interrupted = await asyncio.wait_for(
            self.interrupt_thread(thread_key, "operator requested a sandbox reset"),
            constants._RESET_INTERRUPT_TIMEOUT_S,
        )
    except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
        logger.error(
            "reset: interrupt did not land for thread %s (timed out or errored); "
            "releasing the sandbox anyway, so a turn that is still live loses it "
            "mid-run and replays on a fresh sandbox",
            thread_key,
            exc_info=True,
        )
    else:
        if interrupted:
            logger.info(
                "reset: interrupted the live turn on thread %s before releasing",
                thread_key,
            )
        else:
            logger.info("reset: no live runner to interrupt on thread %s", thread_key)
    # Serialize the teardown against `_route_and_start` for this thread by
    # holding the same route lock the turn path holds (#734). Both the lock
    # acquisition and the release run on their own bounds, so a wedged turn
    # or control plane cannot park the maintenance tick here; a lock that
    # cannot be taken in time propagates as a release failure, same as a
    # timed-out release.
    lock_key = self._config.lock_key(thread_key)
    # This outer bound shadows `acquire`'s own configured
    # `lock_acquire_timeout_s` (45s by default): whichever fires first wins,
    # and `_RESET_LOCK_ACQUIRE_TIMEOUT_S` is the shorter one, so it is the
    # effective bound on the reset path. Both raise `TimeoutError` (the
    # inner one as `LockAcquireTimeout`), so the caller sees one shape.
    token = await asyncio.wait_for(
        self._lock.acquire(lock_key), constants._RESET_LOCK_ACQUIRE_TIMEOUT_S
    )
    try:
        released = await asyncio.wait_for(
            asyncio.to_thread(self._substrate.release, thread_key),
            constants._RESET_RELEASE_TIMEOUT_S,
        )
        if released and self._workspace is not None:
            await asyncio.to_thread(self._workspace.release, thread_key)
        return released
    finally:
        await self._lock.release(lock_key, token)


async def interrupt_agent(self: Kernel, agent_id: uuid.UUID) -> int:
    """Interrupt every live turn belonging to an agent (kill switch). Returns
    the number of turns signalled. The kill flag stays set (the API owns it),
    so new runs are refused by the is_killed check until resume.

    Threads are signalled concurrently, and each interrupt is individually
    bounded to `_KILL_INTERRUPT_TIMEOUT_S` (#742): a wedged runner on one
    thread must not delay -- let alone block -- the interrupt reaching the
    agent's other live threads. A timed-out or otherwise failed interrupt is
    logged and does not stop the rest of the fan-out; there is no fallback
    release to run afterward on this path (unlike `release_thread`), so the
    failure is surfaced via logging rather than swallowed."""
    threads = list(self._active_by_agent.get(agent_id, set()))
    await self._cancel_settling_work_items(agent_id=agent_id)

    async def _interrupt_one(thread_key: str) -> bool:
        try:
            return await asyncio.wait_for(
                self.interrupt_thread(thread_key, f"agent {agent_id} killed by operator"),
                constants._KILL_INTERRUPT_TIMEOUT_S,
            )
        except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
            logger.error(
                "kill: interrupt did not land for thread %s of agent %s (timed out "
                "or errored); continuing to signal its other live threads",
                thread_key,
                agent_id,
                exc_info=True,
            )
            return False

    results = await asyncio.gather(*(_interrupt_one(key) for key in threads))
    # A run parked for approval has no live turn to signal, but the kill
    # still ends it here, so the sweeper can reclaim the request (#3564).
    self._forget_held_work_items(agent_id=agent_id, reason=f"agent {agent_id} killed")
    signalled = sum(results)
    logger.info("kill: interrupted %d live turn(s) for agent %s", signalled, agent_id)
    return signalled
