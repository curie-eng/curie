"""The queue seam between the dispatcher and the worker (F1).

``QueuedTurn`` (from ``aci_protocol``) is the normalized job the dispatcher
enqueues onto the Valkey Stream and the worker consumes. The model was promoted
into the frozen ACI package (issue #7) so the contract is shared across three
languages and guarded by the schema-compat gate rather than hand-mirrored. This
module owns only the Valkey Stream *transport* of that model (a transport detail
that stays out of the frozen package): the single-``payload`` wire encoding plus
the dedupe guard.

Wire encoding: a Stream entry carries the model as a single ``payload`` field
holding ``model_dump_json()``. A one-field JSON blob keeps the seam explicit and
versionable (add fields without reshaping the Stream schema) and lets the worker
reconstruct the model with ``from_stream_fields``.

Dedupe (idempotency, detailed-architecture 2b rule 5): the idempotency key is the
delivery's ``event_id`` (the Slack event id for the Slack adapter). A retried
delivery must not enqueue twice. We guard with a Valkey ``SET <dedupe_key> 1 NX
EX <ttl>`` before enqueuing: the first delivery claims the key and enqueues; a
retry finds the key present and is dropped. This is chosen over stream-side
dedupe because it is O(1), TTL-bounded (no unbounded dedupe set to prune), and
does not require scanning the Stream.
"""

import logging
from typing import Any, Protocol, cast, runtime_checkable

from aci_protocol import STREAM_PAYLOAD_FIELD, QueuedTurn, parse_queued_turn
from curie_telemetry import inject_trace_context, operation_span, record_metric
from opentelemetry.trace import SpanKind, StatusCode

from .config import DispatcherConfig

logger = logging.getLogger(__name__)


@runtime_checkable
class StreamPublisher(Protocol):
    """The stream-broker port (producer side): append an entry + dedupe-claim.

    Issue #284 / ADR-0027. The producer's whole coupling to the broker is these
    two verbs: ``xadd`` (append the one-``payload`` entry to the stream) and a
    ``SET NX EX`` dedupe-claim (``set``). Typing the producer against this
    Protocol makes a future non-redis broker a drop-in on the write side. The
    consumer side has its own port (``StreamBroker`` in the worker). ``redis.Redis``
    structurally satisfies this, so it is the one backing today with no adapter.
    The verbs are typed permissively to match redis-py's signatures.
    """

    def xadd(self, name: Any, fields: Any) -> Any:
        """Append an entry to ``name``; returns the assigned stream id."""
        ...

    def set(self, name: Any, value: Any, *, nx: bool = ..., ex: Any = ...) -> Any:
        """``SET`` with ``NX``/``EX`` — the dedupe-claim beside the stream."""
        ...


def to_stream_fields(turn: QueuedTurn) -> dict[str, str]:
    """Render the byte-identical payload plus optional transport trace context."""
    fields = {STREAM_PAYLOAD_FIELD: turn.model_dump_json()}
    inject_trace_context(fields)
    return fields


def from_stream_fields(fields: dict[str, str]) -> QueuedTurn:
    """Reconstruct a turn from a Stream entry's field map (the worker's entry point)."""
    return parse_queued_turn(fields[STREAM_PAYLOAD_FIELD])


def claim_event(redis_client: "StreamPublisher", config: "DispatcherConfig", event_id: str) -> bool:
    """Claim a Slack event id for processing, returning True on the first claim.

    Uses ``SET NX EX`` so exactly one delivery of a given event id wins; retries
    see the key already set and get False, which the caller treats as a duplicate
    to drop.
    """
    attributes = {"service.name": "curie-dispatcher", "source": "dispatcher"}
    with operation_span(
        "curie.queue.dedupe",
        kind=SpanKind.INTERNAL,
        attributes=attributes,
    ) as span:
        try:
            claimed = redis_client.set(
                config.dedupe_key(event_id),
                "1",
                nx=True,
                ex=config.dedupe_ttl_seconds,
            )
        except Exception:
            span.set_status(StatusCode.ERROR)
            span.add_event("queue.dedupe.failed", {"outcome": "failure"})
            logger.error("queue dedupe decision failed")
            raise
        outcome = "claimed" if claimed else "duplicate"
        span.add_event("queue.dedupe.decided", {"outcome": outcome})
        logger.info("queue dedupe decision: %s", outcome)
    return bool(claimed)


def enqueue(redis_client: "StreamPublisher", config: "DispatcherConfig", turn: QueuedTurn) -> str:
    """Append the normalized turn to the Valkey Stream, returning its Stream id."""
    attributes = {"service.name": "curie-dispatcher", "source": "dispatcher"}
    error: Exception | None = None
    stream_id: object | None = None
    outcome = "failure"
    with operation_span(
        "curie.queue.enqueue",
        kind=SpanKind.PRODUCER,
        attributes=attributes,
    ) as span:
        try:
            # Build inside the producer span so the carrier names this exact
            # async handoff while leaving QueuedTurn's JSON bytes untouched.
            fields = cast("dict[Any, Any]", to_stream_fields(turn))
            stream_id = redis_client.xadd(config.stream, fields)
        except Exception as exc:  # preserve the queue's original exception shape
            error = exc
            span.set_status(StatusCode.ERROR)
            span.add_event("queue.enqueue.failed", {"outcome": "failure"})
        else:
            outcome = "success"
            span.add_event("queue.enqueued", {"outcome": "success"})
            logger.info("turn enqueued")

    metric_attributes = {**attributes, "outcome": outcome}
    record_metric("curie.queue.enqueue", attributes=metric_attributes)
    if error is not None:
        raise error
    assert stream_id is not None
    record_metric(
        "curie.turn.accepted",
        attributes={
            "service.name": "curie-dispatcher",
            "source": "dispatcher",
            "outcome": "accepted",
        },
    )
    return _as_str(stream_id)


def _as_str(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)
