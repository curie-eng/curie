"""The `curie dev tier-diff` entry point: read run artifacts, print the diff.

Kept separate from the diff engine so the engine stays a pure function of its
inputs and is testable without touching a filesystem or an exit code.

Exit codes follow the CLI contract in ``AGENTS.md`` (0 success / 1 failure /
2 usage / 3 transient). Finding a behavioral difference is a SUCCESSFUL run of
this command, so it exits 0: the command did its job. Turning a finding into a
non-zero exit is opt-in through ``--fail-on-change``, which is what a CI gate
would pass. Reusing code 3 for "found something" would have been convenient and
wrong, since 3 means transient and a retry would not change a diff.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from pydantic import ValidationError

from .tierdiff import Observation, diff_observations
from .tierdiff_render import render_markdown, render_terminal

_USAGE_ERROR = 2
_FAILURE = 1


def _load(path: Path) -> Observation:
    """Parse one observation artifact from disk.

    Args:
        path: Path to a JSON artifact holding one bundle version's per-tier runs.

    Returns:
        The parsed observation.

    Raises:
        SystemExit: With the usage exit code, if the file is missing, is not JSON,
            or does not validate. A malformed artifact is a caller mistake rather
            than a transient condition, so it is not retryable and must not be
            reported as one.
    """
    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        raise SystemExit(f"tier-diff: no such artifact: {path}") from None
    except json.JSONDecodeError as exc:
        raise SystemExit(f"tier-diff: {path} is not valid JSON: {exc}") from None
    try:
        return Observation.model_validate(raw)
    except ValidationError as exc:
        raise SystemExit(f"tier-diff: {path} is not a valid run artifact:\n{exc}") from None


def build_parser() -> argparse.ArgumentParser:
    """Return the argument parser for the tier-diff entry point."""
    parser = argparse.ArgumentParser(
        prog="curie dev tier-diff",
        description="Diff a candidate bundle's behavior against the deployed bundle, across the ladder.",
    )
    parser.add_argument(
        "--deployed",
        required=True,
        type=Path,
        metavar="ARTIFACT",
        help="Run artifact for the bundle currently deployed (the baseline).",
    )
    parser.add_argument(
        "--candidate",
        required=True,
        action="append",
        type=Path,
        metavar="ARTIFACT",
        help=(
            "Run artifact for the candidate bundle. Repeat the flag to supply "
            "repeats of the same version; repeats are what make the flake rate "
            "measurable, and a single repeat cannot detect flake at all."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("terminal", "markdown", "json"),
        default="terminal",
        help="terminal (default), markdown for a pull request comment, or json.",
    )
    parser.add_argument(
        "--fail-on-change",
        action="store_true",
        help="Exit 1 when any row is noteworthy. For a CI gate; off by default.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the tier-diff entry point.

    Args:
        argv: Command line arguments, defaulting to ``sys.argv[1:]``.

    Returns:
        The process exit code.
    """
    args = build_parser().parse_args(argv)
    baseline = _load(args.deployed)
    candidates = [_load(path) for path in args.candidate]
    try:
        report = diff_observations(baseline, candidates)
    except ValueError as exc:
        print(f"tier-diff: {exc}", file=sys.stderr)
        return _USAGE_ERROR

    if args.format == "json":
        print(report.model_dump_json(indent=2))
    elif args.format == "markdown":
        print(render_markdown(report))
    else:
        print(render_terminal(report))

    if args.fail_on_change and report.noteworthy:
        return _FAILURE
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
