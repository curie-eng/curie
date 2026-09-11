"""Summarize observed verification phase durations from /implement run-state files.

The summarizer reads the optional ``timings`` block on each ``e2e.evidence``
entry of a ``.state.json`` run-state file and reports per-phase totals. It is
observed-only: a phase that was never measured is reported as unknown and is
excluded from every count, sum, median and percentile.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

PHASES = ("startup", "tests", "cleanup", "retries", "external_wait")
PHASE_FIELDS = frozenset({"seconds", "started_at", "completed_at", "count"})
# A ``seconds`` value and a timestamp pair are two independent measurements of
# the same phase. Sub-second disagreement is clock and rounding noise; anything
# larger means one of the two is describing a different span, and guessing which
# one is authoritative would be exactly the invention this tool exists to refuse.
AGREEMENT_TOLERANCE_SECONDS = 1.0
# A timestamp with no time component is a date, not an instant. Accepting one
# would let two identical dates subtract to a confident 0.0 seconds -- a guessed
# midnight wearing the provenance of a real measurement.
_TIME_COMPONENT = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")
# Identity fields. Without all three, unrelated candidates would collapse into
# one duplicate group under a shared empty key.
IDENTITY_FIELDS = ("tier", "command", "commit")


class TimingRecordError(ValueError):
    """A run-state timing record is malformed and cannot be trusted."""


@dataclass(frozen=True)
class PhaseTiming:
    """One phase of one evidence row, with how its duration was established."""

    seconds: float | None
    provenance: str

    def as_dict(self) -> dict[str, Any]:
        return {"seconds": self.seconds, "provenance": self.provenance}


@dataclass(frozen=True)
class TimingRecord:
    """One evidence row, reduced to its structured, non-free-text fields."""

    source: str
    tier: str
    command: str
    commit: str
    mode: str
    outcome: str
    timings: dict[str, PhaseTiming]
    retry_count: int | None

    @property
    def identity(self) -> tuple[str, str, str]:
        return (self.tier, self.command, self.commit)

    @property
    def sort_key(self) -> tuple[str, ...]:
        return (self.command, self.commit, self.tier, self.mode, self.outcome, self.source)


def _fail(source: Path, phase: str, reason: str) -> TimingRecordError:
    return TimingRecordError(f"{source}: phase '{phase}': {reason}")


def _number(source: Path, phase: str, value: object) -> float | None:
    # bool is an int subclass; a flag is never a duration.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except OverflowError as exc:
        # A huge JSON integer has no float representation; it is not a duration.
        raise _fail(source, phase, "seconds is too large to be a duration") from exc
    if not math.isfinite(number):
        raise _fail(source, phase, "seconds must be a finite number")
    return number


def _timestamp(source: Path, phase: str, field: str, value: object) -> datetime:
    if not isinstance(value, str):
        raise _fail(source, phase, f"{field} must be an ISO 8601 string")
    if not _TIME_COMPONENT.match(value):
        raise _fail(source, phase, f"{field} must carry a time component, not a date alone")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _fail(source, phase, f"{field} is not a valid ISO 8601 timestamp") from exc


def _string(source: Path, field: str, value: object) -> str:
    if not isinstance(value, str):
        raise TimingRecordError(f"{source}: evidence field '{field}' must be a string")
    return value


def _identity(source: Path, field: str, entry: dict[str, Any]) -> str:
    if field not in entry:
        raise TimingRecordError(f"{source}: evidence field '{field}' is required")
    value = _string(source, field, entry[field]).strip()
    if not value:
        raise TimingRecordError(f"{source}: evidence field '{field}' must not be empty")
    return value


def _parse_phase(source: Path, phase: str, raw: object) -> tuple[PhaseTiming, int | None]:
    if raw is None:
        return PhaseTiming(None, "unknown"), None
    if not isinstance(raw, dict):
        raise _fail(source, phase, "must be an object or null")
    unknown_fields = sorted(set(raw).difference(PHASE_FIELDS))
    if unknown_fields:
        raise _fail(source, phase, f"unknown fields: {', '.join(unknown_fields)}")

    count: int | None = None
    if "count" in raw:
        if phase != "retries":
            raise _fail(source, phase, "count is only meaningful on the retries phase")
        raw_count = raw["count"]
        if isinstance(raw_count, bool) or not isinstance(raw_count, int):
            raise _fail(source, phase, "count must be an integer")
        if raw_count < 0:
            raise _fail(source, phase, "count must not be negative")
        count = raw_count

    declared = raw.get("seconds")
    seconds: float | None = None
    if declared is not None:
        seconds = _number(source, phase, declared)
        if seconds is None:
            raise _fail(source, phase, "seconds must be a number")
        if seconds < 0:
            raise _fail(source, phase, "seconds must not be negative")

    measured: float | None = None
    if raw.get("started_at") is not None or raw.get("completed_at") is not None:
        if raw.get("started_at") is None or raw.get("completed_at") is None:
            raise _fail(source, phase, "started_at and completed_at must both be present")
        started = _timestamp(source, phase, "started_at", raw["started_at"])
        completed = _timestamp(source, phase, "completed_at", raw["completed_at"])
        if (started.tzinfo is None) != (completed.tzinfo is None):
            # Never guess the missing zone: the offset could be anything.
            raise _fail(
                source,
                phase,
                "started_at and completed_at must both be timezone-aware or both naive",
            )
        measured = (completed - started).total_seconds()
        if measured < 0:
            raise _fail(source, phase, "completed_at precedes started_at")

    if seconds is not None and measured is not None:
        if abs(seconds - measured) > AGREEMENT_TOLERANCE_SECONDS:
            raise _fail(
                source,
                phase,
                f"seconds ({seconds}) disagrees with the timestamp span ({measured})",
            )
        return PhaseTiming(measured, "timestamps"), count
    if measured is not None:
        return PhaseTiming(measured, "timestamps"), count
    if seconds is not None:
        return PhaseTiming(seconds, "seconds"), count
    # No measurement of any kind: stays unknown, never zero, and never inherits
    # a value from a sibling phase or a neighbouring row.
    return PhaseTiming(None, "unknown"), count


def _parse_entry(source: Path, entry: object) -> TimingRecord:
    if not isinstance(entry, dict):
        raise TimingRecordError(f"{source}: evidence entries must be objects")
    raw_timings = entry.get("timings")
    if raw_timings is None:
        raw_timings = {}
    if not isinstance(raw_timings, dict):
        raise TimingRecordError(f"{source}: evidence field 'timings' must be an object")
    unknown_phases = sorted(set(raw_timings).difference(PHASES))
    if unknown_phases:
        raise TimingRecordError(
            f"{source}: phase '{unknown_phases[0]}': not a recognised verification phase"
        )

    timings: dict[str, PhaseTiming] = {}
    retry_count: int | None = None
    for phase in PHASES:
        timing, count = _parse_phase(source, phase, raw_timings.get(phase))
        timings[phase] = timing
        if count is not None:
            retry_count = count

    return TimingRecord(
        # Only the bare file name reaches the summary; a local absolute path is
        # an environment detail, and error messages keep the full path instead.
        source=source.name,
        tier=_identity(source, "tier", entry),
        command=_identity(source, "command", entry),
        commit=_identity(source, "commit", entry),
        mode=_string(source, "mode", entry.get("mode", "")),
        outcome=_string(source, "outcome", entry.get("outcome", "")),
        timings=timings,
        retry_count=retry_count,
    )


def _load_file(path: Path) -> list[TimingRecord]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise TimingRecordError(f"{path}: could not be read as JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise TimingRecordError(f"{path}: run-state must be a JSON object")
    # Type-check before defaulting: `or {}` would turn `false` into a silent
    # zero-row success, quietly dropping a corrupt file from a combined summary.
    e2e = document.get("e2e", {})
    if e2e is None:
        e2e = {}
    if not isinstance(e2e, dict):
        raise TimingRecordError(f"{path}: run-state field 'e2e' must be an object")
    evidence = e2e.get("evidence", [])
    if evidence is None:
        evidence = []
    if not isinstance(evidence, list):
        raise TimingRecordError(f"{path}: run-state field 'e2e.evidence' must be a list")
    return [_parse_entry(path, entry) for entry in evidence]


def _expand(target: Path) -> list[Path]:
    if target.is_dir():
        return sorted(target.glob("*.state.json"))
    return [target]


def load_records(targets: Sequence[Path | str]) -> list[TimingRecord]:
    """Load every evidence row from the given run-state files or directories."""
    records: list[TimingRecord] = []
    for target in targets:
        for path in _expand(Path(target)):
            records.extend(_load_file(path))
    return records


def _percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank percentile over an already sorted, non-empty sample."""
    rank = max(1, math.ceil(fraction * len(values)))
    return values[rank - 1]


def _phase_totals(records: Sequence[TimingRecord], phase: str) -> dict[str, Any]:
    observed = sorted(
        timing.seconds
        for timing in (record.timings[phase] for record in records)
        if timing.seconds is not None
    )
    unknown_count = len(records) - len(observed)
    if not observed:
        return {
            "observed_count": 0,
            "unknown_count": unknown_count,
            "total_seconds": None,
            "median_seconds": None,
            "p90_seconds": None,
        }
    return {
        "observed_count": len(observed),
        "unknown_count": unknown_count,
        "total_seconds": math.fsum(observed),
        "median_seconds": statistics.median(observed),
        "p90_seconds": _percentile(observed, 0.9),
    }


def _duplicate_groups(records: Sequence[TimingRecord]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[TimingRecord]] = {}
    for record in records:
        grouped.setdefault(record.identity, []).append(record)
    # Repeated (tier, command, commit) rows are reported as a group rather than
    # collapsed: two runs of the same command are two observations, and merging
    # them would silently discard one real measurement.
    return [
        {
            "tier": tier,
            "command": command,
            "commit": commit,
            "row_count": len(members),
            "sources": sorted(member.source for member in members),
        }
        for (tier, command, commit), members in sorted(grouped.items())
        if len(members) > 1
    ]


def summarize(records: Sequence[TimingRecord]) -> dict[str, Any]:
    """Build the deterministic, observed-only summary for the loaded rows."""
    ordered = sorted(records, key=lambda record: record.sort_key)
    declared_retries = [record.retry_count for record in ordered if record.retry_count is not None]
    return {
        "row_count": len(ordered),
        "rows": [
            {
                "source": record.source,
                "tier": record.tier,
                "command": record.command,
                "commit": record.commit,
                "mode": record.mode,
                "outcome": record.outcome,
                "timings": {phase: record.timings[phase].as_dict() for phase in PHASES},
            }
            for record in ordered
        ],
        "phase_totals": {phase: _phase_totals(ordered, phase) for phase in PHASES},
        # No row declared a retry count, so the number of retries is unknown --
        # which is not the same claim as "there were zero retries".
        "retry_total_count": sum(declared_retries) if declared_retries else None,
        "duplicate_groups": _duplicate_groups(ordered),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("targets", nargs="+", type=Path, help="run-state files or directories")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        summary = summarize(load_records(args.targets))
    except TimingRecordError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(summary, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
