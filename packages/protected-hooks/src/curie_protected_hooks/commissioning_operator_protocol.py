"""Provisioner/control process events and markers for the commissioning operator, ADR 0236.

@spec PROTECTED-COMMISSIONING-5. The provisioner sequences the control process
with immutable marker ConfigMaps in the control namespace and reads the control
process's structured events from the owned control Pod's log. Events are
reports, never authority: the provisioner corroborates each one with its own
producers, and the guard path uses only the signed observation channel. No
event or marker names an acceptance, permit, Qualification or Readiness.

Both sets are closed. A malformed event line refuses the whole log rather than
being skipped, so a reader never acts on a partial report.
"""

from __future__ import annotations

import json
import re
from typing import Any, Literal, NoReturn

EVENT_PREFIX = "CURIE_COMMISSIONING_EVENT "
MARKER_LABEL = "curietech.ai/commissioning-marker"
EVENTS = frozenset(
    {
        "SERVER_READY",
        "PRIMITIVE_OBSERVED",
        "CAMPAIGN_PHASE",
        "CAMPAIGN_RESULT",
        "GUARD_RESULT",
        "ERROR",
        "FINISHED",
    }
)
# Markers in the order the provisioner creates them.
MARKERS = (
    "campaign-start",
    "candidates-revoked",
    "finish-guard",
    "control-exit",
)
DATA_MARKERS = frozenset({"campaign-start"})
_MARKER_PREFIX = "commissioning-marker-"
_LINE_CAP = 65536
_GUARD = frozenset({"call", "outcome", "latency_ms"})
_NAMESPACE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


class OperatorProtocolRefused(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Commissioning operator protocol refused")


def _refuse() -> NoReturn:
    raise OperatorProtocolRefused() from None


def _require(value: bool) -> None:
    if not value:
        _refuse()


def _no_floats(value: Any) -> None:
    _require(type(value) is not float)
    if type(value) is dict:
        for member in value.values():
            _no_floats(member)
    elif type(value) is list:
        for member in value:
            _no_floats(member)


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = dict(pairs)
    _require(len(result) == len(pairs))
    return result


def canonical(value: Any) -> str:
    _no_floats(value)
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )


def _check(name: str, value: Any) -> None:
    _require(type(name) is str and name in EVENTS and type(value) is dict)
    if name == "GUARD_RESULT":
        _require(value.keys() == _GUARD)
        _require(type(value["call"]) is int and value["call"] >= 1)
        _require(value["outcome"] in ("pass", "refused"))
        _require(type(value["latency_ms"]) is int and value["latency_ms"] >= 0)


def encode_event(name: str, value: dict[str, Any]) -> str:
    try:
        _check(name, value)
        line = EVENT_PREFIX + name + " " + canonical(value)
        _require(len(line) <= _LINE_CAP)
        return line
    except OperatorProtocolRefused:
        raise
    except Exception:  # noqa: BLE001
        _refuse()


def guard_result(*, call: int, outcome: Literal["pass", "refused"], latency_ms: int) -> str:
    """The control process's report of one guard call and its measured latency."""
    return encode_event(
        "GUARD_RESULT", {"call": call, "outcome": outcome, "latency_ms": latency_ms}
    )


def parse_events(text: str) -> tuple[tuple[str, dict[str, Any]], ...]:
    """Every prefixed line must be a known, canonical event; other lines are ignored."""
    try:
        _require(type(text) is str)
        events = []
        for line in text.splitlines():
            if not line.startswith(EVENT_PREFIX):
                continue
            _require(len(line) <= _LINE_CAP)
            name, sep, body = line[len(EVENT_PREFIX) :].partition(" ")
            _require(sep == " ")
            value = json.loads(body, object_pairs_hook=_pairs, parse_constant=lambda _: _refuse())
            _check(name, value)
            _require(canonical(value) == body)
            events.append((name, value))
        return tuple(events)
    except OperatorProtocolRefused:
        raise
    except Exception:  # noqa: BLE001
        _refuse()


def marker_document(
    marker: str, *, namespace: str, data: dict[str, str] | None = None
) -> dict[str, Any]:
    """One immutable marker ConfigMap; only declared markers carry data."""
    _require(type(marker) is str and marker in MARKERS)
    _require(type(namespace) is str and _NAMESPACE.fullmatch(namespace) is not None)
    _require((data is not None) == (marker in DATA_MARKERS))
    document: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": _MARKER_PREFIX + marker,
            "namespace": namespace,
            "labels": {MARKER_LABEL: marker},
        },
        "immutable": True,
    }
    if data is not None:
        _require(type(data) is dict and all(type(v) is str for v in data.values()))
        document["data"] = dict(data)
    return document
