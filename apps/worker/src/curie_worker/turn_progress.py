"""The kernel's side of the deliberate progress ingress (ADR 0130).

A person's Slack turn gets a per-turn capability to report progress: a
``turn.progress`` sandbox token whose subject is the turn chain's
``progress_id``, and the API route it is good for. The kernel sends both to the
runner as two runner control headers on ``POST /v1/event``; they are not ACI
fields, and ADR 0130 leaves the frozen ACI unchanged. The runner's ``progress``
tool posts each command to the API, which appends it to the chain's inbox
stream, and while the kernel consumes the turn a ``ProgressPump`` applies the
inbox to the chain's durable record (``curie_worker.progress``).

Everything here is best effort by construction: a failure to plan, mint, open
or pump is logged and the turn runs without progress. Progress never fails a
turn.

The header names, the scope, the route and the inbox entry are frozen with the
runner and the API in ``tests/vectors/turn-progress-capability.json``. The
rules are in the worker README's "Deliberate progress (ADR 0130)" section.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Final

from aci_protocol import QueuedTurn, TurnSource
from aci_protocol.turn import CLUSTER_MESSAGE_ADAPTER
from channel_protocol.progress import ProgressCommand
from channel_protocol.reply import ReplyTarget
from pydantic import ValidationError
from redis.exceptions import ConnectionError as ValkeyConnectionError
from redis.exceptions import TimeoutError as ValkeyTimeoutError

from . import sandbox_token
from .binding import SANDBOX_TOKEN_TTL_SECONDS
from .config import WorkerConfig
from .progress import ProgressStore, progress_id_for
from .reply_sink import TargetRoute

logger = logging.getLogger(__name__)

URL_HEADER: Final = "X-Curie-Progress-Url"
TOKEN_HEADER: Final = "X-Curie-Progress-Token"
TOKEN_SCOPE: Final = "turn.progress"
ROUTE: Final = "/v1/turn-progress/{progress_id}"

# How often a live turn's pump looks at its inbox, how much it takes at once,
# and how long the drain after the stream ends may hold the turn's completion.
PUMP_INTERVAL_S: Final = 0.5
PUMP_BATCH: Final = 64
PUMP_DRAIN_TIMEOUT_S: Final = 5.0

_SLACK_KIND: Final = "slack"
_INBOX_FIELDS: Final = frozenset({"command", "epoch", "seq"})
# epoch and seq travel through the store's Lua, whose numbers are doubles.
_MAX_POSITION: Final = 2**53 - 1
_TRANSIENT = (ValkeyConnectionError, ValkeyTimeoutError, OSError)


def progress_eligible(qevent: QueuedTurn, *, factory_work_item: bool) -> bool:
    """Whether a turn is a person's Slack turn, the only kind that reports progress.

    A job (webhook or cron), a targetless hook, a turn on another channel kind,
    a factory execution and a ``curie cluster message`` relay turn are not.
    An approval resume of a person's turn arrives as a Slack turn and passes.
    """

    handle = qevent.reply_handle
    return (
        handle is not None
        and qevent.source is TurnSource.SLACK
        and handle.kind == _SLACK_KIND
        and handle.adapter != CLUSTER_MESSAGE_ADAPTER
        and not factory_work_item
    )


@dataclass(frozen=True)
class TurnProgressPlan:
    """The chain one delivery reports progress on, named before any turn starts.

    ``root_event_id`` is the event a fresh chain derives from, and None for an
    approval resume, whose chain already exists and is only followed.
    """

    progress_id: str
    thread_key: str
    root_event_id: str | None


async def plan_turn_progress(
    store: ProgressStore,
    qevent: QueuedTurn,
    thread_key: str,
    *,
    factory_work_item: bool,
    resume: bool,
) -> TurnProgressPlan | None:
    """The delivery's chain, or None when the turn reports no progress.

    A fresh turn names ``progress_id_for(thread_key, event_id)``, so a retry of
    the same event names the same record. A resume follows only the pointer its
    suspended turn wrote; with that pointer expired there is no chain, and a
    resume never derives one of its own (ADR 0130 section 2).
    """

    if not progress_eligible(qevent, factory_work_item=factory_work_item):
        return None
    if not resume:
        return TurnProgressPlan(
            progress_id=progress_id_for(thread_key, qevent.event_id),
            thread_key=thread_key,
            root_event_id=qevent.event_id,
        )
    try:
        progress_id = await store.resolve_chain(qevent.event_id)
    except Exception:  # noqa: BLE001 - progress never fails a turn
        logger.warning("progress chain lookup failed for %s", qevent.event_id, exc_info=True)
        return None
    if progress_id is None:
        return None
    return TurnProgressPlan(progress_id=progress_id, thread_key=thread_key, root_event_id=None)


def capability_url(config: WorkerConfig, progress_id: str) -> str:
    """The API route a chain's capability is good for, as the runner reaches it."""

    base = config.runner_facing_api_base_url.rstrip("/")
    return base + ROUTE.format(progress_id=progress_id)


def mint_capability(
    config: WorkerConfig, progress_id: str, *, now: float | None = None
) -> dict[str, str] | None:
    """The runner control headers for one turn start, or None without an API key.

    A fresh token per start: each is bound to the chain and the scope, and the
    API rate limits it per token, so a retried turn starts with its own budget.
    """

    if not config.api_key:
        return None
    issued = int(now if now is not None else time.time())
    return {
        URL_HEADER: capability_url(config, progress_id),
        TOKEN_HEADER: sandbox_token.mint(
            config.api_key,
            agent=progress_id,
            scope=TOKEN_SCOPE,
            exp=issued + SANDBOX_TOKEN_TTL_SECONDS,
        ),
    }


def resume_event_id_for(approval_id: object) -> str:
    """The event id of an approval's resume turn.

    The API's ``resumequeue.resume_event_id`` mints it, and both the resolve
    and the expiry paths stamp it; ``kernel._approval_id_from_resume_event`` is
    its inverse on this side.
    """

    return f"approval-{approval_id}-resolved"


async def link_progress_resume(
    store: ProgressStore, plan: TurnProgressPlan, approval_id: object
) -> None:
    """Point a paused turn's resume event at its chain, so the resume continues it."""

    resume_event_id = resume_event_id_for(approval_id)
    try:
        linked = await store.link_resume(resume_event_id, plan.progress_id)
    except Exception:  # noqa: BLE001 - progress never fails a turn
        logger.warning(
            "progress chain %s was not linked to %s", plan.progress_id, resume_event_id,
            exc_info=True,
        )
        return
    if not linked:
        logger.info(
            "progress chain %s is gone or %s is linked elsewhere; the resume shows no progress",
            plan.progress_id,
            resume_event_id,
        )


def parse_inbox_entry(fields: Mapping[str, str]) -> tuple[ProgressCommand, int, int] | None:
    """One inbox entry as the API wrote it, or None when it is not one."""

    if set(fields) != _INBOX_FIELDS:
        return None
    try:
        command = ProgressCommand.model_validate_json(fields["command"])
        epoch = int(fields["epoch"])
        seq = int(fields["seq"])
    except (ValidationError, ValueError):
        return None
    if not (1 <= epoch <= _MAX_POSITION and 1 <= seq <= _MAX_POSITION):
        return None
    return command, epoch, seq


class ProgressPump:
    """Applies one chain's inbox to its record while one turn is consumed.

    It reads after the record's ``inbox_cursor``, applies each entry at the
    entry's ``(epoch, seq)`` and moves the cursor past it, so a later turn of
    the chain (an approval resume) starts where this one stopped. The record's
    rules decide what an entry does: a duplicate, an out-of-order command and
    a platform-only state are refused there, not here.
    """

    def __init__(
        self,
        store: ProgressStore,
        *,
        progress_id: str,
        route: TargetRoute,
        target: Callable[[], ReplyTarget],
        render: bool,
        cursor: str,
        interval_s: float = PUMP_INTERVAL_S,
        batch: int = PUMP_BATCH,
    ) -> None:
        self._store = store
        self._progress_id = progress_id
        self._route = route
        self._target = target
        self._render = render
        self._cursor = cursor
        self._interval_s = interval_s
        self._batch = batch
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Drain what the turn left in the inbox, bounded, then stop.

        A caller that is itself being cancelled is not held for the drain: the
        pump is cancelled at once and the next turn of the chain applies what
        was left, from the cursor.
        """

        self._stop.set()
        task = self._task
        if task is None:
            return
        current = asyncio.current_task()
        if current is not None and current.cancelling():
            task.cancel()
            return
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=PUMP_DRAIN_TIMEOUT_S)
        except TimeoutError:
            logger.warning(
                "progress pump for %s did not drain within %.0fs; stopping it",
                self._progress_id,
                PUMP_DRAIN_TIMEOUT_S,
            )
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        except Exception:  # noqa: BLE001 - _run logs its own failures
            pass

    async def _run(self) -> None:
        while True:
            stopping = self._stop.is_set()
            try:
                read = await self._pump_once()
            except Exception:  # noqa: BLE001 - progress never fails a turn
                logger.warning(
                    "progress pump for %s could not read its inbox",
                    self._progress_id,
                    exc_info=True,
                )
                read = 0
            if read >= self._batch:
                continue
            if stopping:
                return
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval_s)

    async def _pump_once(self) -> int:
        entries = await self._store.read_inbox(
            self._progress_id, after=self._cursor, count=self._batch
        )
        for entry_id, fields in entries:
            try:
                await self._apply(entry_id, fields)
            except _TRANSIENT:
                raise
            except Exception:  # noqa: BLE001 - one bad entry must not stall the chain
                logger.warning(
                    "progress entry %s of %s could not be applied; skipping it",
                    entry_id,
                    self._progress_id,
                    exc_info=True,
                )
            self._cursor = entry_id
            await self._store.advance_cursor(self._progress_id, entry_id)
        return len(entries)

    async def _apply(self, entry_id: str, fields: Mapping[str, str]) -> None:
        parsed = parse_inbox_entry(fields)
        if parsed is None:
            logger.warning(
                "progress entry %s of %s is malformed; skipping it", entry_id, self._progress_id
            )
            return
        command, epoch, seq = parsed
        outcome = await self._store.apply_model_command(
            self._progress_id,
            command,
            epoch=epoch,
            seq=seq,
            route=self._route,
            target=self._target(),
        )
        logger.info(
            "progress %s entry %s: %s%s",
            self._progress_id,
            entry_id,
            outcome.status,
            f" ({outcome.reason})" if outcome.reason else "",
        )
        if outcome.deliveries and not self._render:
            await self._store.discard_deliveries(outcome.deliveries)


async def start_progress_pump(
    store: ProgressStore,
    plan: TurnProgressPlan,
    *,
    route: TargetRoute,
    target: Callable[[], ReplyTarget],
    render: bool,
    interval_s: float = PUMP_INTERVAL_S,
) -> ProgressPump | None:
    """Open a fresh chain's record, then start its pump; None when neither can.

    A fresh chain is opened here, when a turn's stream is consumed, and not
    when the delivery is planned, so an event that only steers a live turn
    opens nothing. Opening is idempotent, so a retried turn reopens nothing.
    """

    try:
        if plan.root_event_id is not None:
            await store.open_chain(
                plan.thread_key, plan.root_event_id, answer_ref=target().reply_ref
            )
        record = await store.read(plan.progress_id)
    except Exception:  # noqa: BLE001 - progress never fails a turn
        logger.warning("progress pump for %s did not start", plan.progress_id, exc_info=True)
        return None
    pump = ProgressPump(
        store,
        progress_id=plan.progress_id,
        route=route,
        target=target,
        render=render,
        cursor=record.inbox_cursor if record is not None else "",
        interval_s=interval_s,
    )
    pump.start()
    return pump
