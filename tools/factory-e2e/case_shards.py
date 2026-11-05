from __future__ import annotations

import json
import math
from pathlib import Path
from statistics import median

DURATIONS_FILE = Path(__file__).with_name("tests") / "case_durations.json"


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
    for node in sorted(node_ids, key=lambda node: (-weights.get(node, fallback), node)):
        shard = min(range(count), key=lambda index: (totals[index], index))
        assignments[node] = shard
        totals[shard] += weights.get(node, fallback)
    return [[node for node in node_ids if assignments[node] == shard] for shard in range(count)]
