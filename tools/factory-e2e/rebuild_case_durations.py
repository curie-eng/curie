"""Rebuild case timings: uv run python tools/factory-e2e/rebuild_case_durations.py LOG..."""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

from case_shards import DURATIONS_FILE

PHASE_ROW = re.compile(r"(?:^|\s)(\d+(?:\.\d+)?)s\s+(?:setup|call|teardown)\s+(\S+::[^\r\n]+)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "logs", type=Path, nargs="+", help="Downloaded gh run view --log files, oldest first"
    )
    parser.add_argument("--output", type=Path, default=DURATIONS_FILE)
    args = parser.parse_args()
    weights: dict[str, float] = {}
    for path in args.logs:
        measured: dict[str, float] = defaultdict(float)
        for line in path.read_text().splitlines():
            match = PHASE_ROW.search(line)
            if match:
                seconds, node = match.groups()
                measured[node.strip()] += float(seconds)
        if not measured:
            parser.error(f"No pytest duration rows in {path}")
        weights.update(measured)
    rounded = {node: round(value, 2) for node, value in sorted(weights.items())}
    args.output.write_text(json.dumps(rounded, indent=2) + "\n")
    print(f"Wrote {len(weights)} case durations")


if __name__ == "__main__":
    main()
