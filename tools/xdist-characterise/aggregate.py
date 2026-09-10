"""Union the tests that failed across repeated parallel runs of the suite.

#2230's parallelism comment measured `-n 4` three times and found two different
tests failing, one per run, and said so plainly: that is not one reproducible
conflict, it is two points inside a boundary nobody has drawn. It then named
what would make parallelism shippable, and the first item is this: run the
suite many times and collect every test that EVER fails, rather than reasoning
from a handful of samples.

This reads the JUnit XML each attempt wrote and answers one question: which
tests failed, and in how many of the attempts. A test that fails in every
attempt is a real breakage under parallelism. A test that fails in two of
twenty is the interesting kind, the one a three-sample study reports as a
different test each time, and the one that has to be pinned rather than
debugged away.

Attempt count comes from the files present, not from a flag, so an attempt
whose job died without uploading cannot silently deflate a rate.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any
from xml.etree import ElementTree


def _test_id(case: ElementTree.Element) -> str:
    """Rebuild the node id pytest would accept as a selector.

    pytest's JUnit writer puts the source path in `file` and the dotted module
    path in `classname`. `file` is the half that round-trips back into a
    command line, so it is preferred; `classname` is the fallback for a writer
    that omits it.
    """
    name = case.get("name", "")
    path = case.get("file")
    if path:
        return f"{path}::{name}"
    return f"{case.get('classname', '')}::{name}"


def _failures(report: Path) -> set[str]:
    """Every test in one attempt that failed or errored.

    A skip is not a failure and a passing rerun does not erase one: this is the
    per-attempt set that the union is built from.
    """
    try:
        tree = ElementTree.parse(report)
    except ElementTree.ParseError as error:
        raise ValueError(f"{report} is not parsable JUnit XML: {error}") from error

    failed: set[str] = set()
    for case in tree.iter("testcase"):
        if case.find("failure") is not None or case.find("error") is not None:
            failed.add(_test_id(case))
    return failed


def characterise(reports: list[Path]) -> dict[str, Any]:
    if not reports:
        raise ValueError("no JUnit XML reports were found, so nothing can be characterised")

    counts: dict[str, int] = defaultdict(int)
    for report in sorted(reports):
        for test in _failures(report):
            counts[test] += 1

    attempts = len(reports)
    # Most failures first, then by node id, so a rerun of the same data prints
    # the same report and a diff between two characterisations is readable.
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return {
        "attempts": attempts,
        "flaky": [
            {"test": test, "failed": count, "of": attempts}
            for test, count in ranked
        ],
    }


def _render(result: dict[str, Any]) -> str:
    attempts = result["attempts"]
    flaky = result["flaky"]
    if not flaky:
        return f"No test failed in any of the {attempts} attempts."

    lines = [f"{len(flaky)} test(s) failed in at least one of {attempts} attempts:", ""]
    for entry in flaky:
        lines.append(f"  {entry['failed']:>3}/{attempts}  {entry['test']}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--reports",
        type=Path,
        required=True,
        help="directory holding one JUnit XML file per attempt",
    )
    parser.add_argument("--json", type=Path, help="also write the result as JSON here")
    arguments = parser.parse_args(argv)

    reports = sorted(arguments.reports.rglob("*.xml"))
    try:
        result = characterise(reports)
    except ValueError as error:
        print(f"xdist characterisation error: {error}", file=sys.stderr)
        return 1

    print(_render(result))
    if arguments.json:
        arguments.json.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
