from __future__ import annotations

import asyncio
import hashlib
import math
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from aci_protocol import (
    Event,
    QueuedTurn,
)

if TYPE_CHECKING:
    from .core import Kernel

from . import clock, constants
from .log import logger


@dataclass(frozen=True)
class TurnMemoryGrant:
    """What the kernel needs to mint a turn's memory write credential (ADR-0188).

    Built in the routing block from the resolved deployment and the turn's
    binding, and handed to ``_attempt``, which mints a fresh credential per
    attempt onto the runner ``Event`` (never the boot env). Absent for a
    targetless turn and an eval-isolated one."""

    resolved: Any
    kind: str
    address: str
    thread_key: str


class ReclaimPreflightUnsafe(RuntimeError):
    """A transferred delivery could not be proven safe to re-execute.

    Raised by the reclaim preflight when the previous owner's runner still
    reports (or cannot deny) a live turn after the bounded interrupt-and-wait.
    Raising leaves the stream entry PENDING, which is the fail-closed answer
    ADR-0131 requires: "a replacement does not run beside a possibly active
    turn." It is deliberately NOT a retryable classification -- there is no
    attempt to classify, because no attempt was started.
    """


@dataclass(frozen=True)
class _MemoryMint:
    """What this attempt's runner call needs to mint its memory credential.

    Set for the routing of one attempt, and read where the turn actually opens
    (``_start_turn_under_hook_control``) or steers. Minting there, rather than
    when the attempt starts, ties the credential's expiry to the stream deadline
    the runner call uses, after the sandbox claim (ADR-0188)."""

    qevent: QueuedTurn
    grant: TurnMemoryGrant


@dataclass
class _AttemptMemoryTurns:
    """The memory write credentials one ``_attempt`` minted (#3776).

    ``_with_memory_token`` records each turn claim here, so the attempt can tell
    the API those turns are over when it ends. ``steered`` means the attempt
    folded into another attempt's live turn, which keeps using the credential;
    ``steered_into`` names that live turn (``_live_memory_turn``), or is None
    when the runner did not say which turn it was. ``live_turns`` are the
    runner turns this attempt opened, so it also closes the steers that joined
    each of them."""

    agent_id: uuid.UUID | None = None
    minted: list[tuple[uuid.UUID, str]] = field(default_factory=list)
    steered: bool = False
    steered_into: str | None = None
    live_turns: list[str] = field(default_factory=list)


# The bound on one close call, so a hung API cannot keep a close task (and the
# shutdown grace below) waiting. The binding client's own timeout is 5 s.
_MEMORY_CLOSE_TIMEOUT_S = 6.0

# How long shutdown waits for closes still in flight.
_MEMORY_CLOSE_SHUTDOWN_GRACE_S = 2.0


def _live_memory_turn(epoch: object) -> str | None:
    """The name a steer and its live turn's owner both use for that turn (#3776).

    The runner's turn epoch, which the owner gets when it opens the turn and a
    steer reads from the pre-steer status. Hashed, because the epoch is the
    turn's private timeout credential and this name goes into a Valkey key."""

    if not isinstance(epoch, str) or not epoch:
        return None
    return hashlib.sha256(epoch.encode()).hexdigest()[:32]


def _note_live_memory_turn(turn: object) -> None:
    """Record a runner turn the current attempt opened, for its steers' close."""

    record = constants._MEMORY_TURNS.get()
    live = _live_memory_turn(getattr(turn, "turn_epoch", None))
    if record is not None and live is not None:
        record.live_turns.append(live)


async def _close_memory_turn(
    close: Callable[[uuid.UUID, str], Awaitable[None]], agent: uuid.UUID, turn: str
) -> None:
    """One close call, bounded and logged; it never raises."""

    try:
        async with asyncio.timeout(_MEMORY_CLOSE_TIMEOUT_S):
            await close(agent, turn)
    except TimeoutError:
        logger.warning("memory turn close timed out for %s", turn)
    except Exception as exc:  # noqa: BLE001 -- a close never fails the turn
        logger.warning("memory turn close failed for %s: %s", turn, type(exc).__name__)


def _settled_memory_close(task: asyncio.Task[None]) -> None:
    constants._PENDING_MEMORY_CLOSES.discard(task)
    if not task.cancelled() and task.exception() is not None:
        logger.warning("memory turn close failed: %s", type(task.exception()).__name__)


async def drain_pending_memory_closes(grace_s: float = _MEMORY_CLOSE_SHUTDOWN_GRACE_S) -> None:
    """On worker shutdown, give the closes still in flight a short grace (#3776).

    Then let them go: a close that does not finish leaves its credential to
    expire at the turn's stream deadline, which is the backstop anyway, so
    shutdown is not held up waiting for a slow API."""

    pending = [task for task in constants._PENDING_MEMORY_CLOSES if not task.done()]
    if pending:
        await asyncio.wait(pending, timeout=grace_s)


def _with_memory_token(
    self: Kernel,
    event: Event,
    qevent: QueuedTurn,
    grant: TurnMemoryGrant | None,
    remaining_s: float | None,
    *,
    cap_at: float | None = None,
) -> Event:
    """``event`` with this turn's memory write credential (ADR-0188).

    Call it just before the runner call that opens or steers the turn, with
    the ``remaining_s`` that call gets: the credential expires at that
    call's stream deadline (``RunnerClient.turn_deadline_s``), and no later
    than ``cap_at`` (a wall-clock time) when one is given, as for a steer.

    No credential when there is no grant, the binding cannot mint (a double
    without the method), or the resolver declines (writes off, no key). Nor
    when the budget is spent (``remaining_s <= 0``): the stream then gets
    only a 50 ms floor to fail fast, so there is no turn to cover. ``None``
    is no budget in hand, which is the runner ceiling, not "spent". The
    credential rides the event only (MEMORY-TOKEN-2/3): never the boot env,
    never a log line."""

    unminted = event.model_copy(update={"memory_token": None}) if event.memory_token else event
    if grant is None or self._binding is None:
        return unminted
    mint = getattr(self._binding, "turn_memory_token", None)
    if mint is None:
        return unminted
    if remaining_s is not None and remaining_s <= 0:
        return unminted
    ttl_s = self._runner.turn_deadline_s(remaining_s)
    if cap_at is not None:
        # Whole seconds down, since the binding rounds the lifetime up.
        ttl_s = min(ttl_s, math.floor(cap_at - clock.time.time()))
        if ttl_s <= 0:
            return unminted
    # Unique per mint (#3776): the attempt closes its turn when it ends, so a
    # retry or a continuation needs its own claim, or the close would refuse
    # it. It still starts with the event id, for the API's refusal log.
    turn = f"{qevent.event_id}#{uuid.uuid4().hex[:8]}"
    token = mint(
        grant.resolved,
        kind=grant.kind,
        address=grant.address,
        thread_key=grant.thread_key,
        sender=qevent.author or "",
        turn=turn,
        ttl_s=ttl_s,
    )
    if not token:
        return unminted
    record = constants._MEMORY_TURNS.get()
    agent = getattr(grant.resolved, "agent_id", None)
    if record is not None and isinstance(agent, uuid.UUID):
        record.minted.append((agent, turn))
    return event.model_copy(update={"memory_token": token})


def _record_turn_deadline(self: Kernel, grant: TurnMemoryGrant, remaining_s: float | None) -> None:
    """Remember the stream deadline of a turn just opened, for steer caps."""

    now = clock.time.time()
    for key in [k for k, at in self._turn_deadlines.items() if at <= now]:
        del self._turn_deadlines[key]
    self._turn_deadlines[grant.thread_key] = now + self._runner.turn_deadline_s(remaining_s)


def _settle_memory_turns(self: Kernel, record: _AttemptMemoryTurns) -> None:
    """Close the turns an attempt minted, in the background (#3776).

    The attempt's end does not wait on the API: the closes run as their own
    task, kept in ``_PENDING_MEMORY_CLOSES`` so it is not garbage collected,
    and a cancelled attempt still closes its credential. A failed or timed
    out close never fails the turn: the credential then expires at the
    turn's stream deadline, as before."""

    task = asyncio.ensure_future(self._close_memory_turns(record))
    constants._PENDING_MEMORY_CLOSES.add(task)
    task.add_done_callback(_settled_memory_close)


async def _close_memory_turns(self: Kernel, record: _AttemptMemoryTurns) -> None:
    close = getattr(self._binding, "close_turn_memory", None)
    if close is None:
        return
    turns: list[tuple[uuid.UUID, str]] = []
    if record.steered:
        # The live turn this attempt joined holds the steering credential
        # (the runner's ``MemoryTurn.begin`` on steer) until it ends, so the
        # attempt that owns that turn closes it. A list in Valkey keyed by
        # that live turn, so the owner can be on another worker, and the
        # owner of an earlier turn on the thread never takes it.
        push = getattr(self._markers, "push_steer_memory_turns", None)
        if push is not None and record.minted and record.steered_into is not None:
            by_agent: dict[uuid.UUID, list[tuple[uuid.UUID, str]]] = {}
            for agent, turn in record.minted:
                by_agent.setdefault(agent, []).append((agent, turn))
            for agent, claims in by_agent.items():
                try:
                    await push(agent, record.steered_into, claims)
                except Exception as exc:  # noqa: BLE001 -- falls back to expiry
                    logger.warning(
                        "could not hand a steer's memory turn to its live turn: %s",
                        type(exc).__name__,
                    )
        elif record.minted:
            # The runner did not name its live turn (an older runner, or one
            # booted without a token), so no owner can find this claim. It is
            # refused at its expiry instead.
            logger.info("steer's memory turn left to expire: the live turn was not named")
    else:
        turns.extend(record.minted)
    drain = getattr(self._markers, "drain_steer_memory_turns", None)
    if record.live_turns and drain is not None:
        agents = {agent for agent, _turn in record.minted}
        if record.agent_id is not None:
            agents.add(record.agent_id)
        for agent in agents:
            for live_turn in record.live_turns:
                try:
                    turns.extend(await drain(agent, live_turn))
                except Exception as exc:  # noqa: BLE001 -- falls back to expiry
                    logger.warning(
                        "could not read the steers that joined a turn: %s",
                        type(exc).__name__,
                    )
    if turns:
        # Concurrently, each under its own bound, so one slow close does not
        # hold up the others.
        await asyncio.gather(*(_close_memory_turn(close, agent, turn) for agent, turn in turns))
