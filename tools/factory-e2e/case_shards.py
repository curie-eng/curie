from __future__ import annotations

import json
import math
from pathlib import Path
from statistics import median

DURATIONS_FILE = Path(__file__).with_name("tests") / "case_durations.json"
CASE_GROUPS = ()


def load_durations(path: Path = DURATIONS_FILE) -> dict[str, float]:
    weights = json.loads(path.read_text())
    if not isinstance(weights, dict) or not weights:
        raise ValueError(f"Expected a nonempty duration map in {path}")
    for node, seconds in weights.items():
        if (
            not isinstance(node, str)
            or not isinstance(seconds, (int, float))
            or isinstance(seconds, bool)
            or not math.isfinite(seconds)
            or seconds < 0
        ):
            raise ValueError(f"Invalid case duration in {path}")
    return weights


def assign_case_shards(
    node_ids: list[str], count: int, weights: dict[str, float]
) -> list[list[str]]:
    fallback = median(weights.values())
    totals = [0.0] * count
    assignments = {}
    remaining = set(node_ids)
    units = []
    for group in CASE_GROUPS:
        members = tuple(node for node in group if node in remaining)
        if members:
            units.append(members)
            remaining.difference_update(members)
    units.extend((node,) for node in remaining)

    def weight(unit: tuple[str, ...]) -> float:
        return sum(weights.get(node, fallback) for node in unit)

    for unit in sorted(units, key=lambda unit: (-weight(unit), min(unit))):
        shard = min(range(count), key=lambda index: (totals[index], index))
        assignments.update(dict.fromkeys(unit, shard))
        totals[shard] += weight(unit)
    return [[node for node in node_ids if assignments[node] == shard] for shard in range(count)]
