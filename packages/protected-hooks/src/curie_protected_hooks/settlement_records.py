"""Closed retained proof codecs; no worker or frozen model copies.

@spec PROTECTED-HOOK-LANE-6
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from typing import Any, ClassVar, NoReturn, cast

from channel_protocol.reply import TurnCompleted

from .admission_records import _decode, _encode, _pairs, _require, _validate
from .authority_records import _scalar

MAX_SETTLEMENT_BYTES = 65536
_COMMON = dict(
    schema_version="version",
    event_id="opaque_ref",
    intent_digest="sha256",
    manifest_digest="sha256",
    runtime_id="uuid",
    runtime_generation="generation",
    broker_run_id="run_id",
    envelope_sha256="sha256",
    payload_sha256="sha256",
    committed_stream_id="stream",
    completion_generation="completion_generation",
)
_WITNESS = dict(
    **_COMMON,
    delivery_stream="logical",
    delivery_group="logical",
    delivery_entry_id="stream",
    delivery_owner="opaque_ref",
    delivery_generation="generation",
    completion_record_sha256="sha256",
    completion_context_sha256="sha256",
    marker_value="marker",
    retirement_ref="opaque_ref",
    retirement_proof_sha256="sha256",
)
_CONTEXT = dict(**_COMMON, completion_record_json="completion_json")
_INDEX = dict(schema_version="version", event_id="opaque_ref", intent_digest="sha256")
_RESOLUTION = dict(
    schema_version="version",
    event_id="opaque_ref",
    completion_generation="completion_generation",
    resolution="resolution",
)


def _invalid() -> NoReturn:
    """Detach proof data even from an active ambient exception graph."""
    error = ValueError("invalid protected settlement record")
    try:
        raise error from None
    finally:
        error.__context__ = None
        error.__cause__ = None


def _completion_json_value(raw: str, event_id: str) -> dict[str, Any]:
    """Validate persisted worker shape and actual channel event without importing worker.

    The resolved route's two nullable text fields are internal persisted scalars,
    not a second channel protocol model. Preserve the original JSON string.
    """
    _require(type(raw) is str and len(raw.encode("utf-8")) <= MAX_SETTLEMENT_BYTES)
    value = json.loads(raw, object_pairs_hook=_pairs, parse_constant=lambda _: _invalid())
    _require(
        type(value) is dict and set(value) == {"event_id", "event", "route", "created_at", "done"}
    )
    _require(value["event_id"] == event_id and type(value["done"]) is bool)
    _require(type(value["created_at"]) in (float, int) and math.isfinite(value["created_at"]))
    route = value["route"]
    _require(type(route) is dict and set(route) == {"endpoint", "adapter"})
    _require(all(item is None or type(item) is str for item in route.values()))
    event = TurnCompleted.model_validate(value["event"])
    _require(event.event_id == event_id)
    return cast(dict[str, Any], value)


def validate_completion_json(raw: str, event_id: str) -> dict[str, Any]:
    """Closed safe decoder that detaches raw JSON and ambient input errors."""
    try:
        return _completion_json_value(raw, event_id)
    except Exception:  # noqa: BLE001  Nested channel-model errors contain input values.
        _invalid()


@dataclass(frozen=True, slots=True, repr=False)
class _SettlementRecord:
    _canonical_bytes: bytes
    _schema: ClassVar[dict[str, str]]

    def __post_init__(self) -> None:
        try:
            value = _decode(self._canonical_bytes, MAX_SETTLEMENT_BYTES)
            _require(type(value) is dict and set(value) == set(self._schema))
            for name, kind in self._schema.items():
                item = value[name]
                if kind == "completion_generation":
                    _require(type(item) is str and re.fullmatch("[0-9a-f]{32}", item) is not None)
                elif kind == "marker":
                    _require(type(item) is str and item in {"1", "history_capacity"})
                elif kind == "resolution":
                    _require(type(item) is str and item in {"emitted", "graveyard"})
                elif kind == "completion_json":
                    validate_completion_json(item, value["event_id"])
                elif kind in {"stream", "logical"}:
                    _validate({name: item}, {name: kind})
                else:
                    _scalar(item, kind)
            raw = _encode(value)
            _require(len(raw) <= MAX_SETTLEMENT_BYTES)
            object.__setattr__(self, "_canonical_bytes", raw)
        except Exception:  # noqa: BLE001  Never expose nested validation or content.
            _invalid()

    @property
    def canonical_bytes(self) -> bytes:
        return self._canonical_bytes

    @property
    def digest(self) -> str:
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        return cast(dict[str, Any], json.loads(self._canonical_bytes))

    def __repr__(self) -> str:
        return "<" + type(self).__name__ + ">"


class EventIndex(_SettlementRecord):
    __slots__ = ()
    _schema = _INDEX


class TerminalWitness(_SettlementRecord):
    __slots__ = ()
    _schema = _WITNESS


class CompletionContext(_SettlementRecord):
    __slots__ = ()
    _schema = _CONTEXT


class CompletionResolution(_SettlementRecord):
    __slots__ = ()
    _schema = _RESOLUTION


parse_event_index = EventIndex
parse_terminal_witness = TerminalWitness
parse_completion_context = CompletionContext
parse_completion_resolution = CompletionResolution
