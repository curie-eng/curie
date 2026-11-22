"""The resource gate on an automatic retry of a limit failure (#5427, ADR 0269).

A request records, at its start, the limits actually delivered to that attempt.
When it fails on a limit, its automatic successor is gated: the gate names the
failed dimension and the limit the failed attempt had. The successor starts
only when the limit delivered to its own attempt is strictly larger, so a fresh
run never repeats an exhaustion under the same envelope. A missing observation
never releases a gate.

This module is pure. The SQL lifecycle owns admission, dispatch and the start
transaction; it reads and writes the shapes defined here.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from typing import Any, Final

# The successor condition each gate answers, and the dimension it compares.
GATE_DIMENSIONS: Final[dict[str, str]] = {
    "memory_limit": "memory",
    "ephemeral_storage_limit": "ephemeral_storage",
    "scratch_limit": "scratch",
    "history_capacity": "history_capacity",
    "turn_limit": "max_turns",
    "execution_time_limit": "execution_seconds",
}
GATE_CONDITIONS: Final = frozenset(GATE_DIMENSIONS)

# Where each dimension's value lives in a stored delivered limits record.
_RECORD_KEYS: Final[dict[str, str]] = {
    "memory": "memory_bytes",
    "ephemeral_storage": "ephemeral_storage_bytes",
    "scratch": "scratch_bytes",
    "max_turns": "max_turns",
    "history_capacity": "history_capacity_bytes",
    "execution_seconds": "execution_seconds",
}
# The dimensions the API resolves itself; the worker resolves the others.
_API_DIMENSIONS: Final = frozenset({"history_capacity", "execution_seconds"})

# A successor of a failure with one of these causes, or any gated successor,
# must see its predecessor's sandbox claim gone before it claims its own: the
# failed pod and runner are fenced before a new attempt (#5427).
FENCED_PREDECESSOR_CAUSES: Final = frozenset(
    {
        "sandbox_memory_exhausted",
        "sandbox_storage_exhausted",
        "sandbox_scratch_exhausted",
        "sandbox_node_disrupted",
    }
)

# The volume a scratch exhaustion named, added to the failed request's record
# by its terminal update.
EXHAUSTED_VOLUME_KEY: Final = "exhausted_volume"


def delivered_record(
    worker_limits: Mapping[str, Any], *, history_capacity_bytes: int, execution_seconds: int
) -> dict[str, Any]:
    """The record a start stores, without its runtime epoch.

    ``worker_limits`` is what the worker read from the sandbox it claimed; the
    API adds the two allowances it enforces itself. The start UPDATE adds the
    epoch it writes, so the record and the started attempt cannot disagree.
    """

    return {
        "memory_bytes": worker_limits["memory_bytes"],
        "ephemeral_storage_bytes": worker_limits["ephemeral_storage_bytes"],
        "scratch_bytes": dict(worker_limits["scratch_bytes"]),
        "max_turns": worker_limits["max_turns"],
        "history_capacity_bytes": history_capacity_bytes,
        "execution_seconds": execution_seconds,
    }


def delivered_value(
    record: Mapping[str, Any] | None, dimension: str, volume: str | None
) -> int | None:
    """The positive limit ``record`` holds for ``dimension``, or None when absent."""

    if record is None:
        return None
    value = record.get(_RECORD_KEYS[dimension])
    if dimension == "scratch":
        if volume is None or not isinstance(value, Mapping):
            return None
        value = value.get(volume)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return None
    return value


def gate_for(
    condition: str,
    record: Mapping[str, Any] | None,
    *,
    source_request_id: uuid.UUID,
    source_runtime_epoch: int,
) -> dict[str, Any] | None:
    """The gate a successor blocked on ``condition`` carries, or None.

    None means the failed attempt has no delivered value for the dimension (or
    the scratch volume it exhausted), or its record belongs to another epoch.
    The caller then admits no successor: nothing could ever prove the limit
    was raised.
    """

    dimension = GATE_DIMENSIONS.get(condition)
    if dimension is None or record is None:
        return None
    if record.get("runtime_epoch") != source_runtime_epoch:
        return None
    volume: str | None = None
    if dimension == "scratch":
        named = record.get(EXHAUSTED_VOLUME_KEY)
        if not isinstance(named, str) or not named:
            return None
        volume = named
    failed = delivered_value(record, dimension, volume)
    if failed is None:
        return None
    return {
        "condition": condition,
        "dimension": dimension,
        "volume": volume,
        "failed_limit": failed,
        "source_request_id": str(source_request_id),
        "source_runtime_epoch": source_runtime_epoch,
    }


def gate_met(gate: Mapping[str, Any], record: Mapping[str, Any]) -> bool:
    """Whether ``record`` delivers strictly more of the gated dimension."""

    current = delivered_value(record, str(gate["dimension"]), gate.get("volume"))
    return current is not None and current > int(gate["failed_limit"])


def grant_view(
    gate: Mapping[str, Any], *, history_capacity_bytes: int, execution_seconds: int
) -> dict[str, Any]:
    """The gate as an acquire grant presents it to the worker.

    ``current_limit`` is filled only for the dimensions the API resolves; the
    worker resolves the sandbox dimensions from its own current configuration.
    """

    dimension = str(gate["dimension"])
    current: int | None = None
    if dimension in _API_DIMENSIONS:
        current = history_capacity_bytes if dimension == "history_capacity" else execution_seconds
    return {
        "condition": gate["condition"],
        "dimension": dimension,
        "volume": gate.get("volume"),
        "failed_limit": gate["failed_limit"],
        "current_limit": current,
    }


__all__ = [
    "EXHAUSTED_VOLUME_KEY",
    "FENCED_PREDECESSOR_CAUSES",
    "GATE_CONDITIONS",
    "GATE_DIMENSIONS",
    "delivered_record",
    "delivered_value",
    "gate_for",
    "gate_met",
    "grant_view",
]
