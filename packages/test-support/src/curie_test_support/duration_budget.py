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
    canonical_nodeid,
    merge_base,
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


def function_nodes(source: str, filename: str) -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
    result: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}

    class Functions(ast.NodeVisitor):
        def __init__(self) -> None:
            self.classes: list[str] = []

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            self.classes.append(node.name)
            self.generic_visit(node)
            self.classes.pop()

        def record(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
            result["::".join([*self.classes, node.name])] = node
            self.generic_visit(node)

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            self.record(node)

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            self.record(node)

    Functions().visit(ast.parse(source, filename=filename))
    return result


def overlaps(node: ast.FunctionDef | ast.AsyncFunctionDef, start: int, count: int) -> bool:
    first = min([node.lineno, *(decorator.lineno for decorator in node.decorator_list)])
    return count > 0 and start <= (node.end_lineno or node.lineno) and start + count - 1 >= first


def budget_targets(root: Path, collected: list[str]) -> set[str]:
    event = os.environ.get("GITHUB_EVENT_NAME", "")
    if event and event not in {"pull_request", "pull_request_target"}:
        return set()
    revision = merge_base(root)
    if revision is None:
        return set()
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
    ranges: dict[str, list[tuple[int, int, int, int]]] = {}
    file = ""
    for line in diff.splitlines():
        if line.startswith("+++ "):
            file = line[6:] if line.startswith("+++ b/") else ""
        match = re.match(r"@@ -([0-9]+)(?:,([0-9]+))? \+([0-9]+)(?:,([0-9]+))? @@", line)
        if match and file:
            ranges.setdefault(file, []).append(
                (
                    int(match[1]),
                    int(match[2]) if match[2] is not None else 1,
                    int(match[3]),
                    int(match[4]) if match[4] is not None else 1,
                )
            )
    changed: set[str] = set()
    for file, hunks in ranges.items():
        path = root / file
        if not path.is_file():
            continue
        current = function_nodes(path.read_text(), file)
        for name, node in current.items():
            if any(overlaps(node, start, count) for _, _, start, count in hunks):
                changed.add(file + "::" + name)
        deletions = [(start, count) for start, count, _, added in hunks if added == 0 and count > 0]
        if deletions:
            source = subprocess.run(
                ["git", "show", f"{revision}:{file}"],
                cwd=root,
                text=True,
                capture_output=True,
                check=True,
            ).stdout
            for name, node in function_nodes(source, file).items():
                if (
                    name in current
                    and any(overlaps(node, start, count) for start, count in deletions)
                    and ast.dump(node, include_attributes=False)
                    != ast.dump(current[name], include_attributes=False)
                ):
                    changed.add(file + "::" + name)
    return {nodeid for nodeid in collected if nodeid.split("[", 1)[0] in changed}


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
    except (ValueError, OSError) as error:
        raise pytest.UsageError(f"Cannot load required test-time baseline: {error}") from error
    recorder = DurationObservations(set(baseline["durations"]), config.rootpath)
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
    if report.when == "setup" and file in recorder.collected and file not in recorder.sent:
        recorder.sent.add(file)
        # TestReport.user_properties survives the real xdist worker transport.
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
    def __init__(self, observations: set[str], root: Path) -> None:
        self.observations = observations
        self.root = root
        self.enforced: set[str] = set()
        self.calls: dict[str, float] = {}
        self.collected: dict[str, list[str]] = {}
        self.sent: set[str] = set()

    @pytest.hookimpl(wrapper=True, tryfirst=True)
    def pytest_collection_modifyitems(
        self, config: pytest.Config, items: list[pytest.Item]
    ) -> Generator[None]:
        identities = [canonical_nodeid(item.nodeid) for item in items]
        try:
            self.enforced = budget_targets(self.root, identities)
        except (ValueError, OSError, subprocess.CalledProcessError) as error:
            raise pytest.UsageError(str(error)) from error
        files = {nodeid.split("::", 1)[0] for nodeid in self.observations}
        if not any("::" in argument for argument in config.args):
            for item in items:
                nodeid = canonical_nodeid(item.nodeid)
                file = nodeid.split("::", 1)[0]
                if file in files:
                    self.collected.setdefault(file, []).append(nodeid)
        return (yield)

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        for name, value in report.user_properties:
            if name == "test-time budget target" and value is True:
                self.enforced.add(canonical_nodeid(report.nodeid))
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
