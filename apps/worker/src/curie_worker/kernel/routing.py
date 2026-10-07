from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from aci_protocol import (
    QueuedTurn,
    ReplyHandle,
    TurnSource,
)
from aci_protocol.turn import route_identity
from channel_protocol import (
    parse_scoped_conversation_id,
    scoped_conversation_id,
)
from channel_protocol.reply import (
    REPLY_WIRE_VERSION,
    NavAffordance,
    ReplyAck,
    ReplyTarget,
    ReplyUpdate,
)

from ..behaviorpacks import (
    NavPack,
)
from ..binding import (
    EVAL_ISOLATE_THREAD_PREFIX,
    AmbiguousRoute,
    binding_adapter_for_handle,
)
from ..delivery_lease import DeliveryLease
from ..reply_sink import (
    CLUSTER_MESSAGE_ADAPTER,
    TargetRoute,
)
from ..runner_client import (
    TurnStream,
)
from ..sandbox.types import (
    SandboxHandle,
)
from ..sibling_turns import SIBLING_LIMIT_NOTICE, SiblingLimitReason
from ..workitem_dispatch import (
    parse_work_item_event_id,
)

if TYPE_CHECKING:
    from .core import Kernel

from . import approval_key, constants, log
from .log import logger


def _lifecycle_event(name: str, outcome: str) -> None:
    span = constants._LIFECYCLE_SPAN.get()
    if span is not None:
        span.add_event(name, {"outcome": outcome})


def _record_route(outcome: str) -> None:
    log.record_metric(
        "curie.thread.route",
        attributes={
            "service.name": "curie-worker",
            "source": "worker",
            "outcome": outcome,
        },
    )


def _target_for(qevent: QueuedTurn) -> ReplyTarget:
    """This turn's reply address, in the channel's own (opaque) terms.

    Pure and derived wholly from the turn, so every path that holds a
    ``QueuedTurn`` calls it rather than being handed the answer. The two targets
    that are NOT this turn's -- the policy-routed approval card and the settled
    card, both addressed to a channel the turn may never have come from -- build
    their own ``ReplyTarget`` inline and deliberately do not come through here.
    """
    handle = _reply_handle_for(qevent)
    return ReplyTarget(
        kind=handle.kind,
        address=handle.channel,
        conversation_id=qevent.conversation_id,
        reply_ref=handle.placeholder,
    )


def _thread_key_for(qevent: QueuedTurn) -> str:
    """This turn's INTERNAL thread identity: its channel pair plus its conversation.

    An adapter's conversation id is unique only within one address -- a Slack
    ``thread_ts`` is a per-channel timestamp, and the dispatcher mints it onto
    the turn verbatim -- so with one agent reachable on several addresses
    (ADR-0096 phase 2) the bare id is no longer an identity. Two channels can
    hand the worker the same conversation id for two unrelated conversations.

    This key names every piece of worker-internal state a thread owns: its
    sandbox route (and with it the transcript/history ref the sandbox boots
    against), its per-thread Valkey lock and in-process order lock, and its
    approval-card slot. Keying any of those on the bare id lets the second
    channel's turn adopt the first channel's live session, inheriting its
    transcript and its bundle.

    It is never what goes on the wire. ``ReplyTarget.conversation_id`` keeps the
    BARE adapter id, because adapters thread their replies on it -- the Slack
    sink sends it straight back as ``thread_ts`` -- so a scoped id there would
    reply into a thread that does not exist. Same reason the dispatcher keeps
    minting bare ids: the scoping is the worker's own, and it starts here.

    Each segment is percent-encoded, so no combination can collide with
    another by moving a separator. The route's identity (``route_identity``)
    is a segment after the kind unless it is none or ``default``
    (ADR-0168 decision 4), built by ``channel_protocol.scoped_conversation_id``
    itself, so a pre-ADR key is unchanged and a named one has one more
    segment. The worker only compares the key; the one place it reads a key
    back is quota-pressure ordering (``_is_eval_thread_key``), through the
    canonical inverse.
    """
    if qevent.reply_handle is None and qevent.hook_run is not None:
        # A targetless cron turn (#2963) has no channel pair; its thread belongs
        # to the hook agent. ``@`` is not a legal channel kind, so this key can
        # never collide with a bound (kind, address).
        return scoped_conversation_id("@cron", qevent.hook_run.agent_id, qevent.conversation_id)
    handle = _reply_handle_for(qevent)
    # @spec WORKER-CANARY-3: scope relay state by its binding identity while
    # keeping the handle's adapter reserved for delivery.
    return scoped_conversation_id(
        handle.kind,
        handle.channel,
        qevent.conversation_id,
        identity=route_identity(handle.kind, binding_adapter_for_handle(handle)),
    )


def _is_eval_thread_key(thread_key: str) -> bool:
    """True when ``thread_key`` was built from an eval conversation id.

    The eval paths stamp ``EVAL_ISOLATE_THREAD_PREFIX`` onto the conversation id
    (#1909), and ``_thread_key_for`` scopes it, so the prefix is only visible
    after the canonical inverse. A key that does not parse is not an eval key:
    quota-pressure ordering then falls back to plain expiry order.
    """
    parsed = parse_scoped_conversation_id(thread_key)
    return parsed is not None and parsed.conversation_id.startswith(EVAL_ISOLATE_THREAD_PREFIX)


def _route_from_handle(qevent: QueuedTurn) -> TargetRoute:
    """The route the SERVER minted onto this turn.

    The second of ``TargetRoute``'s two sanctioned sources (EB-B2), and the only
    one the pre-resolution paths can reach. It is trustworthy because no adapter
    writes to the stream at all after ADR-0096 phase 2: the ingress API mints the
    handle from the binding row, and the dispatcher and CLI are first-party.
    """
    handle = _reply_handle_for(qevent)
    return TargetRoute(endpoint=handle.endpoint, adapter=handle.adapter)


def _reply_handle_for(qevent: QueuedTurn) -> ReplyHandle:
    """Return the reply handle of a targeted turn.

    Raises for a targetless turn, so any egress path left unguarded fails closed
    instead of inventing somewhere to send.
    """

    handle = qevent.reply_handle
    if handle is None:
        raise ValueError("targetless turn has no reply target")
    return handle


def _is_targetless(qevent: QueuedTurn) -> bool:
    """A turn with no reply target: a hook run nobody is waiting on (#2963)."""

    return qevent.reply_handle is None


def _check_targetless_shape(qevent: QueuedTurn) -> None:
    """Re-check the wire rule here: direct callers can skip model validation.

    Only a CRON turn carrying its hook run key may omit the reply handle.
    """

    if _is_targetless(qevent) and (qevent.source is not TurnSource.CRON or qevent.hook_run is None):
        raise ValueError("only a cron turn with a hook run may omit its reply target")
    # A targetless turn carries no resume authority (#2963): an id shaped like a
    # work-item wake or an approval resume is an identity violation. It is refused
    # here, before any effect, because settling it through the normal completion
    # path would look up runs and write the done marker keyed by that id, which
    # could finish or defer the UNRELATED run that legitimately owns it.
    if _is_targetless(qevent) and (
        parse_work_item_event_id(qevent.event_id) is not None
        or approval_key._is_approval_resume(qevent.event_id)
    ):
        raise ValueError("a targetless turn may not carry a work-item or resume id")


def _bound_egress_adapter(bound: str | None, handle_adapter: str | None) -> str | None:
    """The adapter a resolved turn replies through (#3475).

    The binding row's adapter wins, except over the reserved relay adapter a
    ``curie cluster message`` turn selects on its handle: since migration 0070
    every Slack row names its identity, and letting that identity replace the
    relay sends the reply to the Slack sink with no endpoint, which is real
    Slack, instead of to the relay the CLI is polling.
    """

    if handle_adapter == CLUSTER_MESSAGE_ADAPTER:
        return handle_adapter
    return bound or handle_adapter


def _nav_affordance(nav: NavPack | None) -> NavAffordance | None:
    """The agent's hub button as the wire carries it, or nothing at all.

    A disabled (or command-less) pack maps to None rather than to an affordance
    with a false flag: absence is the disabled form on the wire, so no adapter
    can render a dead hub button from it.
    """
    if nav is None or not nav.enabled or not nav.hub_command:
        return None
    return NavAffordance(label=nav.hub_label, command=nav.hub_command)


@dataclass
class _RouteResult:
    steered: bool
    handle: SandboxHandle | None = None
    turn: TurnStream | None = None
    # An enabled greeting/help pack matched a provably-fresh thread (no existing
    # route) under the route lock: the canned reply to deliver instead of
    # claiming a sandbox or starting a model turn. None on every other path.
    canned_reply: str | None = None
    # Set only on the new-turn return, when this message's own repository fact
    # selected a workspace the route snapshot taken under the lock did not
    # already carry (#2659). The reply announces it; None on every other path.
    workspace_inferred_repo: str | None = None


def _kernel_target_for(self: Kernel, qevent: QueuedTurn) -> ReplyTarget:
    """This turn's reply target, including any ref minted during the turn.

    Args:
        qevent: The queued turn.

    Returns:
        The target from the wire handle, with ``reply_ref`` replaced by the
        minted ref when this turn posted its own message.
    """
    target = _target_for(qevent)
    if target.reply_ref is not None:
        return target
    minted = self._minted_refs.get(qevent.event_id)
    return target if minted is None else target.model_copy(update={"reply_ref": minted})


def _adopt_ref(self: Kernel, qevent: QueuedTurn, ack: ReplyAck) -> None:
    """Remember a ref an adapter minted for a placeholder-less turn.

    First writer wins: the message that was actually created first is the one
    the rest of the turn edits, so a racing second delivery cannot redirect
    the turn onto a message posted later.

    Args:
        qevent: The queued turn the delivery belonged to.
        ack: The adapter's acknowledgement.
    """
    handle = qevent.reply_handle
    if handle is not None and ack.ref and handle.placeholder is None:
        self._minted_refs.setdefault(qevent.event_id, ack.ref)


async def _reply_for(
    self: Kernel,
    qevent: QueuedTurn,
    route: TargetRoute,
    text: str,
    *,
    best_effort_unreachable: bool = False,
    terminal: bool = True,
) -> ReplyAck:
    """Deliver platform-authored text for this turn, adopting any minted ref.

    The single-shot counterpart of ``_ThrottledReply``: both exist so that no
    caller has to remember the adoption step.

    Args:
        qevent: The queued turn.
        route: This turn's egress route.
        text: The platform-authored text.
        best_effort_unreachable: Swallow an unreachable transport rather than raising.
        terminal: Whether this text is the turn's RESULT for the person
            (#2433). True for every caller that answers, drops, escalates or
            notifies; False for the two booting edits and for the
            not-started notice itself, none of which is a result.

    Returns:
        The adapter's acknowledgement.
    """
    if _is_targetless(qevent) or self._is_factory_work_item_turn(qevent.event_id):
        # No requesting chat egress for a targetless turn (#2963) or a
        # factory execution (#2991). A hook run records its outcome on its
        # row. A factory run is reported by the API's notice writer after
        # the WorkItem settles, so the kernel must not create a competing
        # boot, approval, refusal, escalation, or final message here.
        return ReplyAck()
    if terminal:
        # Marked BEFORE the send, never after, so an exception from ``_reply``
        # cannot skip the mark for text that may already be on the screen.
        self._terminal_reply_attempted.add(qevent.event_id)
    ack = await self._reply(
        self._target_for(qevent),
        route,
        text,
        best_effort_unreachable=best_effort_unreachable,
    )
    self._adopt_ref(qevent, ack)
    return ack


async def _drop_with_message(
    self: Kernel,
    qevent: QueuedTurn,
    route: TargetRoute,
    message: str,
    *,
    lease: DeliveryLease | None = None,
) -> None:
    """Edit the placeholder with a reason and complete the turn (a polite
    drop for an unmapped channel or a paused agent, never a crash)."""
    await self._reply_for(qevent, route, message)
    await self._complete(qevent, route, "dropped", telemetry_outcome="interrupted", lease=lease)


async def _drop_ambiguous_route(
    self: Kernel,
    qevent: QueuedTurn,
    route: TargetRoute,
    exc: AmbiguousRoute,
    *,
    lease: DeliveryLease | None = None,
) -> None:
    """Complete a turn whose route selects several agents' bindings.

    It runs under no deployment and replies through no route: every route
    on the pair belongs to an agent the turn may not be from, so a reply
    through any of them is the misroute being refused (ADR-0168 decision 3).
    """

    logger.error("dropping event %s without a run or a reply: %s", qevent.event_id, exc)
    await self._complete(
        qevent,
        route,
        "dropped",
        telemetry_outcome="interrupted",
        lease=lease,
        hook_outcome="failed",
        hook_reason="target_unbound",
    )


async def _drop_sibling_turn(
    self: Kernel,
    qevent: QueuedTurn,
    route: TargetRoute,
    reason: SiblingLimitReason,
    *,
    lease: DeliveryLease | None = None,
) -> None:
    """Complete a turn the sibling limit refused (ADR-0168 decision 6).

    The placeholder is edited only where an update edits a message the
    reader already sees. On a buffered channel the text would go out as a
    new message, which is the next turn of the exchange this ends. The
    route is the handle's, so the edit is made by the identity that posted
    the placeholder.
    """
    logger.warning("dropping event %s from a sibling identity: %s", qevent.event_id, reason.value)
    handle = qevent.reply_handle
    if (
        handle is not None
        and handle.placeholder is not None
        and self._sink.edits_in_place(handle.kind, route)
    ):
        try:
            await self._reply_for(qevent, route, SIBLING_LIMIT_NOTICE)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - broad catch kept at a failure boundary
            logger.warning(
                "the sibling-limit notice for event %s could not be delivered",
                qevent.event_id,
                exc_info=True,
            )
    await self._complete(qevent, route, "dropped", telemetry_outcome="interrupted", lease=lease)


async def _reply(
    self: Kernel,
    target: ReplyTarget,
    route: TargetRoute,
    text: str,
    *,
    best_effort_unreachable: bool = False,
) -> ReplyAck:
    """One ``reply.update`` carrying platform-authored text."""
    return await self._sink.emit(
        ReplyUpdate(
            version=REPLY_WIRE_VERSION,
            event="reply.update",
            target=target,
            text=text,
        ),
        route=route,
        best_effort_unreachable=best_effort_unreachable,
    )
