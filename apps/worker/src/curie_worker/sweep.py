"""A long scheduled sweep that stays on one sandbox (ADR-0160, #2878).

The pieces outside the kernel: the ``sweep-checkpoint`` fact parser, the
continuation event ids, the coverage read over the API's memory routes, and the
coverage notice text. The kernel glue lives in ``kernel/sweep_slices.py``.

A checkpoint is a sandbox memory FACT written with the per-turn credential
(ADR-0188 decisions 3 and 4), never an append to the memory log. Coverage for a
sweep date is the newest parseable checkpoint fact the API stamped with the
cron sender. The read uses the platform key, which can read every memory, so
the filter in ``SweepCoverage.read`` is the boundary.
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import quote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from .runner_client import _DEFAULT_INTERRUPT_TIMEOUT_S

if TYPE_CHECKING:
    from .cron_loop import TriggerSource

logger = logging.getLogger(__name__)

CHECKPOINT_HEADER = "sweep-checkpoint"
# A slice cut by the delivery budget, with the runner's interrupt accepted or
# not. Any other classification is a failure, not a slice boundary.
BUDGET_CUT_CLASSIFICATIONS = frozenset({"runner-timeout", "runner-timeout-unconfirmed"})
# A continuation waiting on a winding-down turn probes at least this often, so
# the wait ends within this long of the turn ending (#2878 revision 1, R8).
MAX_BUSY_PROBE_INTERVAL_S = 2.0
# Each memory list request's bound; the coverage read issues both concurrently.
READ_TIMEOUT_S = 3.0
# Added to the hook lease renewed at a cron delivery's start. The timed-out
# attempt awaits the interrupt RPC before the budget-cut path can renew the
# lease, then reads coverage; without this margin the lease lapses inside that
# window and the hook's next fire reclaims a sweep that is still deciding.
HOOK_LEASE_START_MARGIN_S = _DEFAULT_INTERRUPT_TIMEOUT_S + READ_TIMEOUT_S + 30.0
# The whole coverage read, zone resolution included (a bundle store read has no
# bound of its own).
READ_TOTAL_TIMEOUT_S = 10.0
# A sweep stops after this many slices in a row that finished no new source, so
# one source may span up to two budget cuts before it is saved.
MAX_STALLED_SLICES = 3
# And after this many slices in all, whatever its progress.
MAX_SWEEP_SLICES = 48

_SWEEP_MARK = "sweep"
_REQUIRED_KEYS = ("sweep", "date", "hook", "covered", "uncovered")
_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_FACT_KEY = re.compile(r"fact-[0-9a-f]{32}")
# Model-authored text reaches a channel through the notice, so only a
# conservative label survives: no markup, no mention syntax, no line breaks.
_LABEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9 ._/#-]{0,63}")


@dataclass(frozen=True)
class SweepCheckpoint:
    """One parsed ``sweep-checkpoint`` statement."""

    sweep: str
    date: date
    hook: str
    covered: tuple[str, ...]
    uncovered: tuple[str, ...]


def _source_list(value: str) -> tuple[str, ...]:
    items = tuple(item.strip() for item in value.split(","))
    items = tuple(item for item in items if item)
    if len(items) == 1 and items[0].lower() == "none":
        return ()
    return items


def parse_checkpoint(statement: object) -> SweepCheckpoint | None:
    """Parse a checkpoint statement, or None for anything else. Never raises."""

    if not isinstance(statement, str):
        return None
    lines = [line.strip() for line in statement.splitlines()]
    lines = [line for line in lines if line]
    if not lines or lines[0].lower() != CHECKPOINT_HEADER:
        return None
    values: dict[str, str] = {}
    for line in lines[1:]:
        key, sep, value = line.partition(":")
        if not sep:
            continue
        key = key.strip().lower()
        if key not in _REQUIRED_KEYS:
            continue
        if key in values:
            return None
        values[key] = value.strip()
    if any(key not in values for key in _REQUIRED_KEYS):
        return None
    if not values["sweep"] or not values["hook"]:
        return None
    if _DATE.fullmatch(values["date"]) is None:
        return None
    try:
        sweep_date = date.fromisoformat(values["date"])
    except ValueError:
        return None
    covered = _source_list(values["covered"])
    uncovered = _source_list(values["uncovered"])
    if set(covered) & set(uncovered):
        return None
    return SweepCheckpoint(
        sweep=values["sweep"],
        date=sweep_date,
        hook=values["hook"],
        covered=covered,
        uncovered=uncovered,
    )


def _ascii_digits(value: str) -> bool:
    return value.isascii() and value.isdecimal()


def parse_continuation(event_id: str) -> tuple[str, int, int, int] | None:
    """``(base, slice_n, covered, stalled)`` for a continuation id, else None.

    ``covered`` is the distinct covered count at enqueue and ``stalled`` the
    slices in a row that finished no new source. Only the last four parts are
    read, so a deferred retry base (#2929) and the colons of a slot timestamp
    stay inside ``base``.
    """

    parts = event_id.rsplit(":", 4)
    if len(parts) != 5 or parts[1] != _SWEEP_MARK:
        return None
    if not all(_ascii_digits(part) for part in parts[2:]):
        return None
    slice_n = int(parts[2])
    if slice_n < 1:
        return None
    return parts[0], slice_n, int(parts[3]), int(parts[4])


def continuation_event_id(event_id: str, covered: int, stalled: int) -> str:
    """The next slice's id: the same base, the next slice number, the counts."""

    carried = parse_continuation(event_id)
    base, slice_n = (event_id, 1) if carried is None else (carried[0], carried[1] + 1)
    return f"{base}:{_SWEEP_MARK}:{slice_n}:{covered}:{stalled}"


def distinct_covered(checkpoint: SweepCheckpoint) -> int:
    """How many different sources a checkpoint covers, ignoring case."""

    return len({source.casefold() for source in checkpoint.covered})


@dataclass(frozen=True)
class SweepRead:
    """One coverage read: the sweep date, and the newest checkpoint for it."""

    date: date | None
    checkpoint: SweepCheckpoint | None


@dataclass(frozen=True)
class SweepRun:
    """What the kernel knows about the hook run a coverage read is for."""

    agent_id: uuid.UUID
    hook: str
    author: str
    slot_utc: datetime
    started_at: datetime
    version_id: uuid.UUID | None
    # (kind, address) of the hook's target binding, or None.
    binding: tuple[str, str] | None


def _aware(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


class SweepCoverage:
    """Reads a hook run's sweep coverage from agent and channel memory."""

    def __init__(
        self,
        *,
        engine: AsyncEngine,
        db_schema: str,
        trigger_source: TriggerSource,
        client: httpx.AsyncClient,
        api_base_url: str,
        api_key: str,
        read_timeout_s: float = READ_TIMEOUT_S,
        read_total_timeout_s: float = READ_TOTAL_TIMEOUT_S,
    ) -> None:
        self._engine = engine
        # Table identifiers are not user input; the schema comes from config.
        self._schema = db_schema
        # The SAME source the cron scheduler fires from, so the sweep date is
        # computed in the zone the slot was scheduled in.
        self._trigger_source = trigger_source
        self._client = client
        self._base = api_base_url.rstrip("/")
        self._api_key = api_key
        self._read_timeout_s = read_timeout_s
        self._read_total_timeout_s = read_total_timeout_s
        # (version, hook) -> zone name. Only a resolved zone is cached.
        self._zones: dict[tuple[uuid.UUID, str], str] = {}

    async def read(self, run: SweepRun) -> SweepRead:
        """The sweep date and its newest checkpoint. Never raises.

        Bounded as a whole by ``read_total_timeout_s``; past it the read has no
        checkpoint, and keeps the sweep date only if it was already resolved.
        """

        known: list[date] = []
        try:
            return await asyncio.wait_for(self._read(run, known), self._read_total_timeout_s)
        except TimeoutError:
            logger.warning(
                "sweep coverage read timed out agent=%s hook=%s after %.1fs",
                run.agent_id,
                run.hook,
                self._read_total_timeout_s,
            )
            return SweepRead(date=known[0] if known else None, checkpoint=None)

    async def _read(self, run: SweepRun, known: list[date]) -> SweepRead:
        try:
            zone = await self._zone(run)
            if zone is None:
                logger.warning(
                    "sweep zone unresolved agent=%s hook=%s; reading no checkpoint",
                    run.agent_id,
                    run.hook,
                )
                return SweepRead(date=None, checkpoint=None)
            sweep_date = run.slot_utc.astimezone(zone).date()
            known.append(sweep_date)
            urls = [f"{self._base}/agents/{run.agent_id}/state/memory"]
            if run.binding is not None:
                kind, address = run.binding
                urls.append(
                    f"{self._base}/agents/{run.agent_id}/state/bindings/"
                    f"{quote(kind, safe='')}/{quote(address, safe='')}/memory"
                )
            sources = await asyncio.gather(*(self._entries(run, url) for url in urls))
            return SweepRead(
                date=sweep_date,
                checkpoint=_newest(run, sweep_date, (e for entries in sources for e in entries)),
            )
        except Exception as exc:  # noqa: BLE001 - a coverage read never fails a turn
            logger.warning(
                "sweep coverage read failed agent=%s hook=%s error_class=%s",
                run.agent_id,
                run.hook,
                type(exc).__name__,
            )
            return SweepRead(date=None, checkpoint=None)

    async def _entries(self, run: SweepRun, url: str) -> list[Any]:
        """One memory list, or nothing when that source cannot be read."""

        try:
            response = await self._client.get(
                url,
                headers={"X-API-Key": self._api_key},
                timeout=self._read_timeout_s,
                follow_redirects=False,
            )
            body = response.json() if response.status_code == 200 else None
        except (httpx.HTTPError, ValueError) as exc:
            logger.warning(
                "sweep memory read failed agent=%s hook=%s error_class=%s",
                run.agent_id,
                run.hook,
                type(exc).__name__,
            )
            return []
        if not isinstance(body, list):
            logger.warning(
                "sweep memory read unusable agent=%s hook=%s status=%s",
                run.agent_id,
                run.hook,
                response.status_code,
            )
            return []
        return body

    async def _zone(self, run: SweepRun) -> ZoneInfo | None:
        """The hook's trigger zone, cached per agent version; None when unresolved."""

        if run.version_id is None:
            return None
        key = (run.version_id, run.hook)
        name = self._zones.get(key)
        if name is None:
            async with self._engine.connect() as connection:
                bundle_ref = (
                    await connection.execute(
                        text(
                            f"SELECT bundle_ref FROM {self._schema}.agent_versions WHERE id = :id"
                        ),
                        {"id": run.version_id},
                    )
                ).scalar_one_or_none()
            if not isinstance(bundle_ref, str) or not bundle_ref:
                return None
            try:
                triggers = await asyncio.to_thread(self._trigger_source.triggers, bundle_ref)
            except Exception as exc:  # noqa: BLE001 - an unreadable bundle is unresolved
                logger.warning(
                    "sweep trigger read failed agent=%s hook=%s error_class=%s",
                    run.agent_id,
                    run.hook,
                    type(exc).__name__,
                )
                return None
            trigger = next(
                (
                    t
                    for t in triggers
                    if isinstance(t, dict) and t.get("type") == "cron" and t.get("name") == run.hook
                ),
                None,
            )
            if trigger is None:
                return None
            # The same default the scheduler fires the slot with.
            name = str(trigger.get("timezone") or "UTC")
        try:
            zone = ZoneInfo(name)
        except (ZoneInfoNotFoundError, ValueError):
            return None
        self._zones[key] = name
        return zone


def _newest(run: SweepRun, sweep_date: date, entries: Iterable[Any]) -> SweepCheckpoint | None:
    """The newest checkpoint this run's cron sender stated for this sweep date."""

    best: SweepCheckpoint | None = None
    best_at: datetime | None = None
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        key, value = entry.get("key"), entry.get("value")
        if not isinstance(key, str) or _FACT_KEY.fullmatch(key) is None:
            continue
        if not isinstance(value, dict) or value.get("author") != run.author:
            continue
        stated_at = _aware(value.get("stated_at"))
        if stated_at is None or stated_at < run.started_at:
            continue
        checkpoint = parse_checkpoint(value.get("statement"))
        if checkpoint is None or checkpoint.hook != run.hook or checkpoint.date != sweep_date:
            continue
        updated_at = _aware(entry.get("updated_at")) or stated_at
        if best_at is None or updated_at > best_at:
            best, best_at = checkpoint, updated_at
    return best


def _label(value: str) -> str | None:
    return value if _LABEL.fullmatch(value) is not None else None


def coverage_notice_text(*, hook: str, outcome: str, read: SweepRead) -> str:
    """The one notice a sweep that stopped short posts. Never plan content."""

    shown_hook = _label(hook)
    named = f'"{shown_hook}"' if shown_hook is not None else "on this hook"
    when = f" for {read.date.isoformat()}" if read.date is not None else ""
    head = f"Scheduled sweep {named}{when} stopped before it finished (run outcome: {outcome})."
    checkpoint = read.checkpoint
    if checkpoint is None:
        return f"{head} Nothing was recorded as covered."
    if not checkpoint.uncovered:
        return f"{head} Not covered: none recorded; the run stopped before it posted its result."
    shown = [label for label in checkpoint.uncovered if _label(label) is not None]
    dropped = len(checkpoint.uncovered) - len(shown)
    listed = ", ".join(shown) if shown else "none that can be shown"
    tail = f" {dropped} source name(s) could not be shown." if dropped else ""
    return f"{head} Not covered: {listed}.{tail}"
