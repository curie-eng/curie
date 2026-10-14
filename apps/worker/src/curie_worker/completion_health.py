"""Read-only completion-outbox health: count, age, retry vs inflight (#2422).

The run-queue PEL (`pending` / `lag`) is a different plane from `turn.completed`
delivery. A turn can be acked with `pending=0` and `lag=0` while its external
reply is still owed. This observer snapshots that outbox and publishes bounded
gauges. It never clears, dead-letters, or relabels a pending record.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from dataclasses import dataclass
from typing import Any, Final

from curie_telemetry import record_metric
from redis.asyncio import Redis

from .config import WorkerConfig
from .markers import MalformedCompletionError, Markers

logger = logging.getLogger(__name__)

STATUS_ARGS: Final[tuple[str, ...]] = (
    "python",
    "-m",
    "curie_worker.completion_health",
    "--json",
)
_DL_SOURCE: Final = "completion-outbox"
_JSON_KEYS: Final[tuple[str, ...]] = (
    "count",
    "oldest_age_s",
    "inflight",
    "retry",
    "terminal",
    "state",
    "degraded",
)


@dataclass(frozen=True)
class CompletionOutboxSnapshot:
    """Operator-safe outbox inventory. Identifiers never enter this record."""

    count: int
    oldest_age_s: float
    inflight: int
    retry: int
    terminal: int
    state: str
    degraded: bool

    def to_json(self) -> dict[str, Any]:
        return snapshot_to_json(
            count=self.count,
            oldest_age_s=self.oldest_age_s,
            inflight=self.inflight,
            retry=self.retry,
            terminal=self.terminal,
            state=self.state,
            degraded=self.degraded,
        )


def snapshot_to_json(
    *,
    count: int,
    oldest_age_s: float,
    inflight: int,
    retry: int,
    terminal: int,
    state: str,
    degraded: bool,
) -> dict[str, Any]:
    """The `--json` body. Keys are closed; run/session ids cannot appear."""

    payload = {
        "count": count,
        "oldest_age_s": oldest_age_s,
        "inflight": inflight,
        "retry": retry,
        "terminal": terminal,
        "state": state,
        "degraded": degraded,
    }
    extra = set(payload) - set(_JSON_KEYS)
    if extra:
        raise ValueError(f"undeclared completion health keys: {sorted(extra)}")
    return payload


def _state(inflight: int, retry: int) -> str:
    if retry:
        return "retry"
    if inflight:
        return "inflight"
    return "empty"


async def _recent_terminal_count(redis: Redis, config: WorkerConfig) -> int:
    """Bounded graveyard sample of completion-outbox dead letters.

    The graveyard mixes run-stream poison with completion settlements. A full
    XRANGE is unbounded, so this reads at most ``completion_sweep_batch`` newest
    rows and counts those tagged ``dl_source=completion-outbox``.
    """

    try:
        rows = await redis.xrevrange(
            config.dead_letter_stream_name(),
            count=config.completion_sweep_batch,
        )
    except Exception:  # noqa: BLE001 - observation must not fail the worker
        return 0
    counted = 0
    for _entry_id, fields in rows or ():
        if not fields:
            continue
        source = fields.get("dl_source", fields.get(b"dl_source"))
        if isinstance(source, bytes):
            source = source.decode()
        if source == _DL_SOURCE:
            counted += 1
    return counted


async def snapshot_completion_outbox(
    markers: Markers,
    redis: Redis,
    config: WorkerConfig,
    *,
    now: float | None = None,
) -> CompletionOutboxSnapshot:
    """Read the pending set. Never mutate it."""

    observed_at = time.time() if now is None else now
    count = int(await redis.scard(config.completions_pending_key()))
    members = await markers.pending_completions(config.completion_sweep_batch)
    stored = await markers.read_completions(sorted(members))
    inflight = 0
    retry = 0
    oldest_retry = 0.0
    oldest_any = 0.0
    for item in stored.values():
        if item is None or isinstance(item, MalformedCompletionError):
            # A member we cannot classify is still owed. Calling it inflight
            # would hide poison behind "not yet due".
            retry += 1
            continue
        age = max(0.0, observed_at - item.record.created_at)
        if age > oldest_any:
            oldest_any = age
        if item.done_flag and age >= config.completion_sweep_grace_s:
            retry += 1
            if age > oldest_retry:
                oldest_retry = age
        else:
            inflight += 1
    sampled = inflight + retry
    if count > sampled:
        # Partial sample of a larger set: fail closed so health cannot go green
        # on the unlucky 64 of a larger owed backlog.
        retry += count - sampled
    terminal = await _recent_terminal_count(redis, config)
    state = _state(inflight, retry)
    return CompletionOutboxSnapshot(
        count=count,
        oldest_age_s=oldest_retry if retry else oldest_any,
        inflight=inflight,
        retry=retry,
        terminal=terminal,
        state=state,
        degraded=retry > 0,
    )


def publish_snapshot(snapshot: CompletionOutboxSnapshot) -> None:
    """Bounded gauges. Labels are service/operation/outcome only."""

    base = {"service.name": "curie-worker", "operation": "observe"}
    for outcome, value in (
        ("inflight", snapshot.inflight),
        ("retry", snapshot.retry),
        ("terminal", snapshot.terminal),
    ):
        record_metric(
            "curie.completion.outbox",
            float(value),
            attributes={**base, "outcome": outcome},
        )
    record_metric(
        "curie.completion.outbox.age",
        float(snapshot.oldest_age_s if snapshot.retry else 0.0),
        attributes={**base, "outcome": "retry"},
    )


async def observe_completion_outbox(
    markers: Markers,
    redis: Redis,
    config: WorkerConfig,
) -> CompletionOutboxSnapshot:
    """Snapshot and publish. Observation failure never gates delivery."""

    try:
        snapshot = await snapshot_completion_outbox(markers, redis, config)
    except Exception as exc:  # noqa: BLE001 - existing broad catch retained
        logger.warning(
            "completion outbox telemetry observation failed (%s)",
            type(exc).__name__,
        )
        # Leave the last published gauges in place. Publishing zeros here would
        # relabel an owed outbox as empty for one scrape.
        return CompletionOutboxSnapshot(
            count=0,
            oldest_age_s=0.0,
            inflight=0,
            retry=0,
            terminal=0,
            state="empty",
            degraded=False,
        )
    try:
        publish_snapshot(snapshot)
    except Exception as exc:  # noqa: BLE001 - existing broad catch retained
        logger.warning(
            "completion outbox metric publish failed (%s)",
            type(exc).__name__,
        )
    return snapshot


async def _read_status(config: WorkerConfig) -> CompletionOutboxSnapshot:
    redis = Redis(**config.valkey_client_kwargs())
    try:
        markers = Markers(redis, config)
        return await snapshot_completion_outbox(markers, redis, config)
    finally:
        await redis.aclose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="curie-worker-completion-health",
        description="Read-only completion-outbox inventory for operator diagnostics (#2422).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="write exactly one JSON object with count, age, and retry/terminal state",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    snapshot = asyncio.run(_read_status(WorkerConfig()))
    if args.json:
        sys.stdout.write(json.dumps(snapshot.to_json(), separators=(",", ":")) + "\n")
    else:
        logger.info(
            "completion outbox count=%d inflight=%d retry=%d terminal=%d "
            "oldest_age_s=%.1f state=%s degraded=%s",
            snapshot.count,
            snapshot.inflight,
            snapshot.retry,
            snapshot.terminal,
            snapshot.oldest_age_s,
            snapshot.state,
            snapshot.degraded,
        )
    return 0


if __name__ == "__main__":  # pragma: no cover - process entry point
    sys.exit(main())
