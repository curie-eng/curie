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


def _is_collection_error(case: ElementTree.Element) -> bool:
    """A worker that never got as far as running tests.

    When xdist workers disagree about what was collected, pytest refuses the
    whole run and writes one entry per worker with no source file, named for
    the worker rather than for a test. Counting those as failing tests is how
    this tool first reported a total collection refusal as "4 flaky tests"
    called `::gw0` through `::gw3`, which is precisely the misreading #2230
    warned about: it makes a suite that never ran look like a suite with a
    handful of loose tests.
    """
    return case.get("file") is None and case.find("error") is not None


def _failures(report: Path) -> tuple[set[str], list[str]]:
    """One attempt's failing tests, and the collection errors that voided it.

    A skip is not a failure and a passing rerun does not erase one: the first
    element is the per-attempt set that the union is built from. The second is
    non-empty only when the attempt never ran, in which case its empty failure
    set means "no evidence", not "nothing went wrong".
    """
    try:
        tree = ElementTree.parse(report)
    except ElementTree.ParseError as error:
        raise ValueError(f"{report} is not parsable JUnit XML: {error}") from error

    failed: set[str] = set()
    collection_errors: list[str] = []
    for case in tree.iter("testcase"):
        if _is_collection_error(case):
            collection_errors.append(case.get("name", "?"))
            continue
        if case.find("failure") is not None or case.find("error") is not None:
            failed.add(_test_id(case))
    return failed, collection_errors


def characterise(reports: list[Path]) -> dict[str, Any]:
    if not reports:
        raise ValueError("no JUnit XML reports were found, so nothing can be characterised")

    counts: dict[str, int] = defaultdict(int)
    never_ran = 0
    for report in sorted(reports):
        failed, collection_errors = _failures(report)
        if collection_errors:
            never_ran += 1
            continue
        for test in failed:
            counts[test] += 1

    attempts = len(reports)
    ran = attempts - never_ran
    # The denominator is the attempts that actually ran. Dividing by every
    # attempt would report a test that failed both of the two usable runs as
    # 2/20, which reads as rare when it is in fact universal.
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return {
        "attempts": attempts,
        "ran": ran,
        "never_ran": never_ran,
        "flaky": [
            {"test": test, "failed": count, "of": ran}
            for test, count in ranked
        ],
    }


def _render(result: dict[str, Any]) -> str:
    attempts = result["attempts"]
    ran = result["ran"]
    never_ran = result["never_ran"]
    flaky = result["flaky"]

    lines: list[str] = []
    if never_ran:
        lines += [
            f"{never_ran} of {attempts} attempts never ran the suite: the workers "
            "disagreed about what was collected, so pytest refused the run.",
            "",
            "That is a determinism defect in the test ids themselves, not "
            "contention between tests, and it has to be fixed before any "
            "statement about parallel flakiness means anything.",
            "",
        ]
    if not ran:
        lines.append("No attempt produced a usable result.")
        return "\n".join(lines)

    if not flaky:
        lines.append(f"No test failed in any of the {ran} attempts that ran.")
        return "\n".join(lines)

    lines += [f"{len(flaky)} test(s) failed in at least one of {ran} attempts that ran:", ""]
    for entry in flaky:
        lines.append(f"  {entry['failed']:>3}/{ran}  {entry['test']}")
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
