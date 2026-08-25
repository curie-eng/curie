"""Dead-letter graveyard watcher (#531): the missing reader on ``<stream>:dead``.

The worker's delivery cap (ADR-0039, #505) moves a permanently-failing entry to
the ``<stream>:dead`` graveyard and acks it off the group -- trading a silent
total consumer stall for an *observable* single loss. But the observable half was
never built: the graveyard is a write-only sink, so a dead-letter emits only the
worker's own log line and nothing platform-side watches it. This watcher is that
reader.

It scans the graveyard from a cursor and emits one structured alert per NEW
dead-letter, so ops alerting (log-based; the repo has no metrics/Prometheus
infra) can key off a single line. Design constraints from the issue:

- **Read-only.** It ``XRANGE``s the stream; no consumer group, no ack, no
  mutation -- safe to run alongside the worker's producer and any other reader.
- **Alert from the observation, not a read-back.** The graveyard is bounded by an
  approximate ``MAXLEN``, so under a flood the oldest rows evict; the watcher
  alerts on each entry as it advances its cursor past it, never by re-reading a
  row that may already be gone.
- **Tail-start.** A fresh boot seeds the cursor at the stream tail so it does not
  re-alert the entire historical graveyard; only dead-letters that arrive while
  it is running are alerted.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from curie_telemetry import operation_span, record_metric
from opentelemetry.trace import SpanKind, StatusCode
from redis.asyncio import Redis

logger = logging.getLogger(__name__)
_monotonic = time.monotonic


def _text(value: Any) -> str:
    """Decode a Valkey value to str. The API's client is created without
    ``decode_responses``, so ids and field maps come back as ``bytes``; a
    ``decode_responses=True`` client (tests) already yields ``str``."""
    return value.decode() if isinstance(value, (bytes, bytearray)) else str(value)


class GraveyardWatcher:
    """Scans ``stream`` for new dead-letter entries and alerts on each."""

    def __init__(self, valkey: Redis, *, stream: str, interval_seconds: float) -> None:
        self._valkey = valkey
        self._stream = stream
        self._interval = interval_seconds
        # Exclusive lower bound for the next scan. Seeded at the tail by
        # ``seed_cursor`` so a boot does not re-alert history; "0-0" (alert on
        # everything) is the safe fallback for an empty/absent stream.
        self._cursor = "0-0"
        self._last_success_monotonic: float | None = None
        # Monotonic count of alerts emitted, exposed for tests and any future
        # health surface.
        self.alerts_emitted = 0

    async def seed_cursor(self) -> None:
        """Set the cursor to the current tail so only NEW dead-letters alert."""
        entries = await self._valkey.xrevrange(self._stream, count=1) or []
        self._cursor = _text(entries[0][0]) if entries else "0-0"

    async def scan_once(self) -> int:
        """Alert every dead-letter after the cursor and advance it. Returns the
        count alerted this pass."""
        entries = await self._valkey.xrange(self._stream, min=f"({self._cursor}", max="+") or []
        for entry_id_raw, fields_raw in entries:
            entry_id = _text(entry_id_raw)
            fields = {_text(k): _text(v) for k, v in (fields_raw or {}).items()}
            self._alert(entry_id, fields)
            self._cursor = entry_id
        return len(entries)

    def _alert(self, entry_id: str, fields: dict[str, str]) -> None:
        self.alerts_emitted += 1
        # A single, greppable ERROR line -- the operational-signal pattern this
        # codebase already uses (structured logs + counters, no Prometheus).
        logger.error(
            "DEAD-LETTER alert: entry %s on %s (original=%s deliveries=%s reason=%s at=%s)",
            entry_id,
            self._stream,
            fields.get("dl_original_id", "?"),
            fields.get("dl_delivery_count", "?"),
            fields.get("dl_reason", "?"),
            fields.get("dl_dead_lettered_at", "?"),
        )

    async def run_forever(self) -> None:
        """Seed at the tail, then scan on the interval forever, surviving errors."""
        try:
            await self.seed_cursor()
        except Exception:
            logger.exception("dead-letter watcher failed to seed its cursor; starting from 0")
        while True:
            await asyncio.sleep(self._interval)
            attributes = {
                "service.name": "curie-api",
                "operation": "graveyard-watcher",
                "role": "background",
            }
            error: Exception | None = None
            with operation_span(
                "curie.background.graveyard-watcher",
                kind=SpanKind.INTERNAL,
                attributes=attributes,
            ) as span:
                try:
                    await self.scan_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    error = exc
                    if hasattr(span, "set_status"):
                        span.set_status(StatusCode.ERROR)
                    span.add_event(
                        "background.pass.failed",
                        {"outcome": "failure", "error.class": type(exc).__name__},
                    )
                else:
                    span.add_event("background.pass.completed", {"outcome": "success"})
            outcome = "failure" if error is not None else "success"
            record_metric(
                "curie.background.loop",
                attributes={**attributes, "outcome": outcome},
            )
            now = _monotonic()
            if error is None:
                self._last_success_monotonic = now
            if self._last_success_monotonic is not None:
                record_metric(
                    "curie.background.last_success.age",
                    max(0.0, now - self._last_success_monotonic),
                    attributes=attributes,
                )
            if error is not None:
                logger.error("dead-letter watcher pass failed (%s)", type(error).__name__)
