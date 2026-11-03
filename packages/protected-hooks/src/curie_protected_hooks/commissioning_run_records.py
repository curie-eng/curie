"""Inert closed commissioning run records, @spec COMMISSIONING-SERIES-4.

Parsing and comparing records establishes no custody, installation freshness,
permit authority, successful trial or acceptance.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, ClassVar

from .authority_records import _object, _scalar
from .commissioning_encoding import (
    _REFUSALS,
    CommissioningRecordInvalid,
    _canonical,
    _decode,
    _require,
)

__all__ = [
    "CommissioningRecordInvalid",
    "RunEvent",
    "RunIntent",
    "RunPlan",
    "parse_run_event",
    "parse_run_intent",
    "parse_run_plan",
    "validate_run_plan",
]

COUNTERS = ("deliveries", "launches", "model_calls", "output_tokens", "spend_microusd")
TRIAL_CASES = (
    "exhaust-deliveries",
    "exhaust-launches",
    "exhaust-model-calls",
    "exhaust-output-tokens",
    "exhaust-spend",
    "live-session-expiry",
    "control-restart",
    "interrupted-closing",
    "broker-restart",
    "final",
)
_LIMITS: dict[str, Any] = {name: "generation" for name in COUNTERS}
_TRIAL: dict[str, Any] = {
    "ordinal": "generation",
    "case": "opaque_ref",
    "limits": _LIMITS,
    "max_duration_ms": "generation",
}
_STATIC: dict[str, Any] = {
    name: "sha256"
    for name in (
        "bootstrap",
        "guard",
        "execution_config",
        "control_profile",
        "projection",
        "image_set",
        "sdk_profile",
        "network_identity",
        "ownership_anchors",
    )
}
_COMMON: dict[str, Any] = {
    "schema_version": "version",
    "run_id": "uuid",
    "installation_id": "uuid",
    "aggregate_limits": _LIMITS,
    "max_elapsed_ms": "generation",
}
_EVENT: dict[str, Any] = {
    "schema_version": "version",
    "run_id": "uuid",
    "revision": "generation",
    "previous_digest": "sha256",
    "kind": "opaque_ref",
    "subject_digest": "sha256",
    "elapsed_ms": "millisecond",
}
_KINDS = frozenset(
    {"prepared", "running", "charged", "started", "settled", "closing", "closed", "failed"}
)
_RUN_KINDS = frozenset({"prepared", "running", "closing", "closed", "failed"})


def _trials(value: Any) -> None:
    _require(type(value) is list and len(value) == len(TRIAL_CASES))
    for ordinal, (row, case) in enumerate(zip(value, TRIAL_CASES, strict=True), 1):
        _object(row, _TRIAL)
        _require(row["ordinal"] == str(ordinal) and row["case"] == case)


def _ordinal(value: Any) -> None:
    if type(value) is str and value == "0":
        return
    _scalar(value, "generation")
    _require(1 <= int(value) <= len(TRIAL_CASES))


def _bytes(raw: bytes, kind: str) -> bytes:
    try:
        if kind == "commissioning_run_intent":
            value = _decode(raw, kind, _COMMON | {"recipe_digest": "sha256"}, {"trials": _trials})
        elif kind == "commissioning_run_plan":
            value = _decode(
                raw,
                kind,
                _COMMON | {"intent_digest": "sha256", "static_binding": _STATIC},
                {"trials": _trials},
            )
        else:
            _require(kind == "commissioning_run_event")
            value = _decode(raw, kind, _EVENT, {"ordinal": _ordinal})
            _require(value["kind"] in _KINDS)
            _require((value["kind"] in _RUN_KINDS) == (value["ordinal"] == "0"))
            _require((value["revision"] == "1") == (value["previous_digest"] == "0" * 64))
            if value["revision"] == "1":
                _require(value["kind"] in {"prepared", "failed"})
            return _canonical(value)
        # Every precommitted campaign burns its complete envelope. Refuse an
        # inventory whose total can never fit, without offering an automatic cap increase.
        for counter in COUNTERS:
            _require(
                sum(int(row["limits"][counter]) for row in value["trials"])
                <= int(value["aggregate_limits"][counter])
            )
        return _canonical(value)
    except _REFUSALS:
        raise CommissioningRecordInvalid() from None


@dataclass(frozen=True, slots=True, repr=False)
class _RunRecord:
    _canonical_bytes: bytes
    _kind: ClassVar[str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "_canonical_bytes", _bytes(self._canonical_bytes, self._kind))

    def __repr__(self) -> str:
        return type(self).__name__ + "()"

    @property
    def canonical_bytes(self) -> bytes:
        """Closed canonical bytes, @spec COMMISSIONING-SERIES-4."""
        return self._canonical_bytes

    @property
    def digest(self) -> str:
        """Content identity only, @spec COMMISSIONING-SERIES-4."""
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        """A fresh nested copy, @spec COMMISSIONING-SERIES-4."""
        result: dict[str, Any] = json.loads(self._canonical_bytes)
        return result


class RunIntent(_RunRecord):
    """Precreation recipe/inventory/bounds, @spec COMMISSIONING-SERIES-4."""

    __slots__ = ()
    _kind = "commissioning_run_intent"


class RunPlan(_RunRecord):
    """Sealed observed static binding; not evidence of truth, @spec COMMISSIONING-SERIES-4."""

    __slots__ = ()
    _kind = "commissioning_run_plan"


class RunEvent(_RunRecord):
    """One inert journal event; not an acceptance record, @spec COMMISSIONING-SERIES-4."""

    __slots__ = ()
    _kind = "commissioning_run_event"


def parse_run_intent(raw: bytes) -> RunIntent:
    """Parse the exact precreation intent, @spec COMMISSIONING-SERIES-4."""
    return RunIntent(raw)


def parse_run_plan(raw: bytes) -> RunPlan:
    """Parse the exact sealed-plan grammar only, @spec COMMISSIONING-SERIES-4."""
    return RunPlan(raw)


def parse_run_event(raw: bytes) -> RunEvent:
    """Parse one event, without trusting its chain, @spec COMMISSIONING-SERIES-4."""
    return RunEvent(raw)


def validate_run_plan(intent: RunIntent, plan: RunPlan) -> None:
    """Require exact intent agreement; authenticate nothing, @spec COMMISSIONING-SERIES-4."""
    _require(type(intent) is RunIntent and type(plan) is RunPlan)
    i, p = intent.as_dict(), plan.as_dict()
    _require(p["intent_digest"] == intent.digest)
    for field in ("run_id", "installation_id", "trials", "aggregate_limits", "max_elapsed_ms"):
        _require(i[field] == p[field])
