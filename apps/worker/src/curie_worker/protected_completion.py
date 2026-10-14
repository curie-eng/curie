"""Context-bound live/cold completion egress, @spec PROTECTED-HOOK-LANE-6."""

from __future__ import annotations

import asyncio
import math
import time

from channel_protocol.reply import ReplyAck, ReplyEvent, TurnCompleted

from .protected_markers import ProtectedMarkers, ProtectedSettlementError
from .reply_sink import (
    DeletedReplyTargetError,
    HttpReplyAdapter,
    ReplySink,
    ReplySinkRouter,
    TargetRoute,
    _BestEffortUnreachableAck,
)


class ProtectedReplySink:
    """Emit only the exact durable completion, recording actual confirmed sends.

    Cold completion never reads a handler ContextVar. Nonterminal reply events
    retain the existing sink behavior; they cannot grant clear authority.
    """

    def __init__(self, markers: ProtectedMarkers, sink: ReplySink) -> None:
        if type(markers) is not ProtectedMarkers or type(sink) not in {
            ReplySinkRouter,
            HttpReplyAdapter,
        }:
            raise ProtectedSettlementError("protected completion unavailable")
        self._markers, self._sink = markers, sink

    async def emit(
        self,
        event: ReplyEvent,
        *,
        route: TargetRoute,
        best_effort_unreachable: bool = False,
    ) -> ReplyAck:
        if not isinstance(event, TurnCompleted):
            return await self._sink.emit(
                event, route=route, best_effort_unreachable=best_effort_unreachable
            )
        owed = await self._markers.repair_completion(event.event_id)
        if (
            owed is None
            or owed.record is None
            or owed.record.event != event
            or owed.record.route != route
            or best_effort_unreachable
        ):
            raise ProtectedSettlementError("protected completion unavailable")
        generation = owed.witness.as_dict()["completion_generation"]
        try:
            ack = await self._sink.emit(
                event, route=owed.record.route, best_effort_unreachable=False
            )
        except DeletedReplyTargetError:
            self._markers._deleted[(event.event_id, generation)] = owed
            raise
        if not isinstance(ack, ReplyAck) or isinstance(ack, _BestEffortUnreachableAck):
            raise ProtectedSettlementError("protected completion unconfirmed")
        self._markers._confirmed[(event.event_id, generation)] = owed
        return ack


async def sweep_protected_completions(
    markers: ProtectedMarkers,
    sink: ProtectedReplySink,
    *,
    limit: int = 64,
    wall_budget_s: float = 5,
) -> int:
    """One bounded maintenance pass; no age discard, payload rebuild or execution."""
    if (
        type(markers) is not ProtectedMarkers
        or type(sink) is not ProtectedReplySink
        or sink._markers is not markers
        or type(limit) is not int
        or not 1 <= limit <= 64
        or not math.isfinite(wall_budget_s)
        or not 0 < wall_budget_s <= 30
    ):
        raise ProtectedSettlementError("protected completion unavailable")
    completed = 0
    deadline = time.monotonic() + wall_budget_s
    try:
        async with asyncio.timeout(wall_budget_s):
            # Across both indexes sample at most the caller's bound.
            repair = await markers._redis.srandmember(
                markers.repair_pending_key(), (limit + 1) // 2
            )
            ordinary = await markers.pending_completions(limit - len(repair or []))
            members = list(dict.fromkeys([*(repair or []), *ordinary]))[:limit]
            for member in members:
                if time.monotonic() >= deadline:
                    break
                event_id = member.decode() if isinstance(member, bytes) else member
                try:
                    owed = await markers.repair_completion(event_id)
                    if owed is None or owed.record is None:
                        continue
                    generation = owed.witness.as_dict()["completion_generation"]
                    try:
                        await sink.emit(owed.record.event, route=owed.record.route)
                    except DeletedReplyTargetError:
                        completed += int(
                            await markers.dead_letter_completion(
                                owed.record,
                                generation=generation,
                                reason=DeletedReplyTargetError.reason,
                            )
                        )
                    else:
                        completed += int(
                            await markers.clear_completion(event_id, generation=generation)
                        )
                except Exception:  # noqa: BLE001  Failure retains exact owed evidence for a later pass.
                    continue
    except TimeoutError:
        pass  # Deadline stops this pass; durable owed proof is unchanged.
    return completed
