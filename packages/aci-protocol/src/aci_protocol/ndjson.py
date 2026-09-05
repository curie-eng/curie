"""NDJSON serialization for the outbound response stream and the queued-turn payload.

The response channel is newline-delimited JSON: one outbound event per line.
These helpers are the single sanctioned encoder and decoder. The decoder
enforces protocol-version compatibility: an event whose ``version`` is missing,
malformed, or not compatible with this build's PROTOCOL_VERSION is rejected with
ProtocolVersionError (naming both versions) rather than being silently accepted,
so a runner speaking an incompatible version fails loudly. Compatibility is same
``major.minor`` under 0.x (see ``version.is_compatible``); a same-major-minor
patch difference is accepted. The decoder threads a reader context so consumers
tolerate unknown fields while producers stay strict on construction.

This module also owns the sanctioned tolerant decode of the queued-turn payload
(``parse_queued_turn``), which crosses the dispatcher->worker queue boundary and
carries no ``version`` field, so it is a pure tolerant field decode with no
version gate -- the same reader-context policy as the NDJSON decoders.
"""

import json
from collections.abc import Iterable, Iterator
from typing import Any

from pydantic import TypeAdapter, ValidationError

from .events import (
    READER_CONTEXT,
    ErrorEvent,
    Final,
    InboundMessage,
    OutboundEvent,
    SideEffectFlag,
    TextDelta,
    ToolNote,
    redact_adoption_credential_error,
)
from .turn import QueuedTurn
from .version import PROTOCOL_VERSION, is_compatible
from .wire import EvalJob

OutboundEventModel = TextDelta | ToolNote | Final | ErrorEvent | SideEffectFlag

_OUTBOUND_ADAPTER: TypeAdapter[OutboundEventModel] = TypeAdapter(OutboundEvent)
_INBOUND_ADAPTER: TypeAdapter[Any] = TypeAdapter(InboundMessage)


class ProtocolVersionError(ValueError):
    """Raised when a decoded event declares an incompatible or malformed version."""


def to_ndjson_line(event: OutboundEventModel) -> str:
    """Encode one outbound event as a single NDJSON line (trailing newline)."""

    return event.model_dump_json() + "\n"


def dump_ndjson(events: Iterable[OutboundEventModel]) -> str:
    """Encode a sequence of outbound events as an NDJSON document."""

    return "".join(to_ndjson_line(e) for e in events)


def parse_ndjson_line(line: str) -> OutboundEventModel:
    """Decode one NDJSON line into a validated outbound event.

    Raises ProtocolVersionError if the line omits ``version``, carries a
    malformed version, or declares a version incompatible with this build's
    PROTOCOL_VERSION. Unknown extra fields are tolerated (the reader context
    loosens the strict-on-construction models). Raises pydantic ValidationError
    for a structurally invalid event.
    """

    raw = json.loads(line)
    if not isinstance(raw, dict):
        raise ProtocolVersionError(f"expected a JSON object per line, got {type(raw).__name__}")
    version: Any = raw.get("version")
    if not is_compatible(version, PROTOCOL_VERSION):
        raise ProtocolVersionError(
            f"unsupported protocol version {version!r}; this build speaks {PROTOCOL_VERSION!r}"
        )
    return _OUTBOUND_ADAPTER.validate_python(raw, context=READER_CONTEXT)


def parse_ndjson(text: str) -> list[OutboundEventModel]:
    """Decode an NDJSON document, skipping blank lines."""

    return list(iter_ndjson(text))


def iter_ndjson(text: str) -> Iterator[OutboundEventModel]:
    """Lazily decode an NDJSON document, skipping blank lines."""

    for line in text.splitlines():
        if line.strip():
            yield parse_ndjson_line(line)


def parse_inbound(raw: str | dict[str, Any]) -> Any:
    """Decode an inbound channel message (event or interrupt) from JSON."""

    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError as exc:
        raise json.JSONDecodeError("malformed inbound JSON", "<redacted>", exc.pos) from None
    try:
        return _INBOUND_ADAPTER.validate_python(data, context=READER_CONTEXT)
    except ValidationError as exc:
        raise redact_adoption_credential_error(exc) from None


def to_inbound_json(message: Any) -> str:
    """Encode an inbound channel message to a JSON string."""

    return _INBOUND_ADAPTER.dump_json(message).decode("utf-8")


def parse_queued_turn(raw: str | bytes) -> QueuedTurn:
    """Decode a queued-turn payload tolerantly (the sanctioned consumer decode).

    This is the queue-boundary counterpart to the NDJSON decoders: it threads the
    same reader context so an unknown field on the turn (or its nested reply
    handle) is tolerated rather than rejected, matching the tolerant-consumer
    policy. QueuedTurn carries no ``version`` field, so there is no version gate
    here -- it is a pure tolerant field decode. Producers stay strict: they
    construct the model directly, where an unknown field is still an error.
    """

    return QueuedTurn.model_validate_json(raw, context=READER_CONTEXT)


def parse_eval_job(raw: str | bytes) -> EvalJob:
    """Decode an eval-job payload tolerantly (the sanctioned consumer decode).

    The ``curie:evals`` counterpart to ``parse_queued_turn``, with the same
    policy for the same reason: a newer API adding an optional field must not
    make an older worker reject the job. Carries no ``version`` field, so there
    is no version gate -- a pure tolerant field decode. Producers stay strict.
    """

    return EvalJob.model_validate_json(raw, context=READER_CONTEXT)


__all__ = [
    "PROTOCOL_VERSION",
    "ProtocolVersionError",
    "to_ndjson_line",
    "dump_ndjson",
    "parse_ndjson_line",
    "parse_ndjson",
    "iter_ndjson",
    "parse_inbound",
    "to_inbound_json",
    "parse_queued_turn",
    "parse_eval_job",
]
