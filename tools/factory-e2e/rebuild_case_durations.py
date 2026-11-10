"""Refresh case timings: uv run python tools/factory-e2e/rebuild_case_durations.py LOG..."""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

from case_shards import DURATIONS_FILE, load_durations

ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
ACTION_PREFIX = re.compile(r"^(?:[^\t]*\t[^\t]*\t)?\d{4}-\d{2}-\d{2}T[\d:.]+Z[ \t]?")
DURATION_HEADER = re.compile(r"^=+ slowest(?: \d+)? durations =+$")
SECTION_HEADER = re.compile(r"^=+ .+ =+$")
PHASE_ROW = re.compile(r"^(\d+(?:\.\d+)?)s\s+(?:setup|call|teardown)\s+(\S+::[^\r\n]+)$")
FAILURE_STATUS = re.compile(r"^(?:FAILED|ERROR)(?:\s|$)")


def durations_from_log(path: Path) -> dict[str, float]:
    lines = [
        ACTION_PREFIX.sub("", ANSI_ESCAPE.sub("", line), count=1)
        for line in path.read_text().splitlines()
    ]
    failures = [line for line in lines if FAILURE_STATUS.search(line)]
    measured: dict[str, float] = defaultdict(float)
    in_durations = False
    for line in lines:
        if DURATION_HEADER.fullmatch(line.strip()):
            in_durations = True
            continue
        if SECTION_HEADER.fullmatch(line.strip()):
            in_durations = False
        if in_durations and (match := PHASE_ROW.fullmatch(line)):
            seconds, node = match.groups()
            measured[node.strip()] += float(seconds)
    return {
        node: seconds
        for node, seconds in measured.items()
        if not any(re.search(rf"(?<!\S){re.escape(node)}(?=\s|$)", line) for line in failures)
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "logs", type=Path, nargs="+", help="Downloaded gh run view --log files, oldest first"
    )
    parser.add_argument("--output", type=Path, default=DURATIONS_FILE)
    args = parser.parse_args()
    weights = load_durations(args.output) if args.output.exists() else {}
    previous = set(weights)
    refreshed: set[str] = set()
    for path in args.logs:
        measured = durations_from_log(path)
        if not measured:
            parser.error(f"No successful pytest duration rows in {path}")
        weights.update({node: round(seconds, 2) for node, seconds in measured.items()})
        refreshed.update(measured)
    args.output.write_text(json.dumps(dict(sorted(weights.items())), indent=2) + "\n")
    print(
        f"Updated {len(previous & refreshed)}, kept {len(previous - refreshed)}, "
        f"added {len(refreshed - previous)} case durations"
    )


if __name__ == "__main__":
    main()
