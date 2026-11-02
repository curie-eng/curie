"""Call-phase budget for the pytest PR shards."""

from __future__ import annotations

import ast
import json
import os
import re
import subprocess
from collections.abc import Generator
from pathlib import Path

import pytest
from _pytest.terminal import TerminalReporter

from curie_test_support.ratchets import (
    BASELINE,
    BUDGET_SECONDS,
    FAIL_SECONDS,
    PRUNE_SECONDS,
    ancestor_baseline,
    canonical_nodeid,
    merge_base,
    nodeid_hash,
    read_baseline,
    write_step_summary,
)

_OBSERVATIONS: pytest.StashKey[DurationObservations] = pytest.StashKey()


def budget_failure(nodeid: str, duration: float, enforced: set[str]) -> str | None:
    nodeid = canonical_nodeid(nodeid)
    if duration > FAIL_SECONDS and nodeid in enforced:
        return (
            f"Call phase {duration:.3f}s exceeds the {BUDGET_SECONDS}s test budget "
            f"with {FAIL_SECONDS - BUDGET_SECONDS}s headroom: {nodeid}"
        )
    return None


def budget_targets(
    root: Path, collected: list[str], known: dict[str, list[str]] | None
) -> set[str]:
    revision = merge_base(root)
    if revision is None:
        return set(collected)
    diff = subprocess.run(
        [
            "git",
            "-c",
            "core.quotePath=false",
            "diff",
            "--no-renames",
            "--unified=0",
            revision,
            "HEAD",
            "--",
            "*.py",
        ],
        cwd=root,
        text=True,
        capture_output=True,
        check=True,
    ).stdout
    ranges: dict[str, list[tuple[int, int]]] = {}
    file = ""
    for line in diff.splitlines():
        if line.startswith("+++ b/"):
            file = line[6:]
        match = re.match(r"@@ -[0-9]+(?:,[0-9]+)? \+([0-9]+)(?:,([0-9]+))? @@", line)
        if match and file:
            start = int(match[1])
            count = int(match[2]) if match[2] is not None else 1
            ranges.setdefault(file, []).append((start, start + max(count, 1) - 1))
    changed: set[str] = set()
    for file, hunks in ranges.items():
        path = root / file
        if not path.is_file():
            continue

        def visit(
            body: list[ast.stmt],
            parents: tuple[str, ...],
            relative: str,
            intervals: list[tuple[int, int]],
        ) -> None:
            for node in body:
                if isinstance(node, ast.ClassDef):
                    visit(node.body, (*parents, node.name), relative, intervals)
                elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    start = min(
                        [node.lineno, *(decorator.lineno for decorator in node.decorator_list)]
                    )
                    end = node.end_lineno or node.lineno
                    if any(low <= end and high >= start for low, high in intervals):
                        changed.add(relative + "::" + "::".join((*parents, node.name)))

        visit(ast.parse(path.read_bytes(), filename=file).body, (), file, hunks)
    known_hashes = (
        {identity for entries in known.values() for identity in entries}
        if known is not None
        else None
    )
    return {
        nodeid
        for nodeid in collected
        if nodeid.split("[", 1)[0] in changed
        or known_hashes is not None
        and nodeid_hash(nodeid) not in known_hashes
    }


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--test-duration-budget",
        action="store_true",
        help="Enforce the PR shard call-phase budget locally.",
    )
    parser.addoption(
        "--test-time-baseline", type=Path, default=None, help="Generated test-time baseline to use."
    )


def pytest_configure(config: pytest.Config) -> None:
    if config.getoption("ci_shard", default=None) is None and not config.getoption(
        "test_duration_budget"
    ):
        return
    path = config.getoption("test_time_baseline") or config.rootpath / BASELINE
    try:
        baseline = read_baseline(path)
        ancestor = (
            ancestor_baseline(config.rootpath)
            if config.getoption("test_time_baseline") is None
            else None
        )
        known = ancestor["known_tests"] if ancestor is not None else None
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        raise pytest.UsageError(f"Cannot load required test-time baseline: {error}") from error
    recorder = DurationObservations(
        set(baseline["durations"]), config.rootpath, known, config.getoption("test_duration_budget")
    )
    config.stash[_OBSERVATIONS] = recorder
    config.pluginmanager.register(recorder)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    report = yield
    if _OBSERVATIONS not in item.config.stash:
        return report
    recorder = item.config.stash[_OBSERVATIONS]
    file = canonical_nodeid(report.nodeid).split("::", 1)[0]
    if report.when == "setup" and file in recorder.known and file not in recorder.sent:
        recorder.sent.add(file)
        # TestReport.user_properties survives the real xdist worker transport.
        report.user_properties.append(("test-time known", json.dumps({file: recorder.known[file]})))
        if file in recorder.collected:
            report.user_properties.append(
                ("test-time collected", json.dumps({file: recorder.collected[file]}))
            )
    if report.when == "call":
        if canonical_nodeid(report.nodeid) in recorder.enforced:
            report.user_properties.append(("test-time budget target", True))
        failure = budget_failure(report.nodeid, report.duration, recorder.enforced)
        if failure:
            report.outcome = "failed"
            report.longrepr = str(report.longrepr) + "\n" + failure if report.longrepr else failure
    return report


class DurationObservations:
    def __init__(
        self,
        observations: set[str],
        root: Path,
        known: dict[str, list[str]] | None,
        enforce_all: bool,
    ) -> None:
        self.observations = observations
        self.root = root
        self.base_known = known
        self.enforce_all = enforce_all
        self.enforced: set[str] = set()
        self.known: dict[str, list[str]] = {}
        self.calls: dict[str, float] = {}
        self.collected: dict[str, list[str]] = {}
        self.sent: set[str] = set()

    @pytest.hookimpl(wrapper=True, tryfirst=True)
    def pytest_collection_modifyitems(
        self, config: pytest.Config, items: list[pytest.Item]
    ) -> Generator[None]:
        identities = [canonical_nodeid(item.nodeid) for item in items]
        self.enforced = (
            set(identities)
            if self.enforce_all
            else budget_targets(self.root, identities, self.base_known)
        )
        files = {nodeid.split("::", 1)[0] for nodeid in self.observations}
        if not any("::" in argument for argument in config.args):
            for item in items:
                nodeid = canonical_nodeid(item.nodeid)
                file = nodeid.split("::", 1)[0]
                self.known.setdefault(file, []).append(nodeid_hash(nodeid))
                if file in files:
                    self.collected.setdefault(file, []).append(nodeid)
        return (yield)

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        for name, value in report.user_properties:
            if name == "test-time budget target" and value is True:
                self.enforced.add(canonical_nodeid(report.nodeid))
            if name == "test-time known" and isinstance(value, str):
                self.known.update(json.loads(value))
            if name == "test-time collected" and isinstance(value, str):
                self.collected.update(json.loads(value))
        if report.when == "call":
            nodeid = canonical_nodeid(report.nodeid)
            self.calls[nodeid] = max(report.duration, self.calls.get(nodeid, 0))

    def pytest_terminal_summary(self, terminalreporter: TerminalReporter) -> None:
        slow = sum(seconds > BUDGET_SECONDS for seconds in self.calls.values())
        terminalreporter.section(f"test-time duration observations: {slow} over budget")
        terminalreporter.write_line(
            "test-time source run: " + os.environ.get("GITHUB_RUN_ID", "local")
        )
        for file, identities in sorted(self.known.items()):
            terminalreporter.write_line(
                "test-time known: " + json.dumps({file: sorted(set(identities))})
            )
        for file, identities in sorted(self.collected.items()):
            terminalreporter.write_line(
                "test-time collected: " + json.dumps({file: sorted(set(identities))})
            )
        for nodeid, seconds in sorted(self.calls.items()):
            if seconds > BUDGET_SECONDS or nodeid in self.observations:
                terminalreporter.write_line(f"{seconds:.6f}s call {nodeid}")
        stale = [
            f"{nodeid}: maximum call {seconds:.6f}s"
            for nodeid, seconds in sorted(self.calls.items())
            if nodeid in self.observations and seconds < PRUNE_SECONDS
        ]
        stale.extend(
            f"{nodeid}: unmatched"
            for nodeid in sorted(self.observations)
            if nodeid.split("::", 1)[0] in self.collected
            and nodeid not in self.collected[nodeid.split("::", 1)[0]]
        )
        write_step_summary(
            "Existing slow tests observed without failing this change:",
            [
                f"{nodeid}: call {seconds:.6f}s"
                for nodeid, seconds in sorted(self.calls.items())
                if seconds > BUDGET_SECONDS and nodeid not in self.enforced
            ],
        )
        write_step_summary(
            "Slow-test observations to prune; regenerate using at least two CI runs:", stale
        )
