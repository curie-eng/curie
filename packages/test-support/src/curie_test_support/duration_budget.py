"""Call-phase budget for the pytest PR shards."""

from __future__ import annotations

import json
import os
from collections.abc import Generator
from pathlib import Path

import pytest
from _pytest.terminal import TerminalReporter

from curie_test_support.ratchets import (
    BASELINE,
    BUDGET_SECONDS,
    PRUNE_SECONDS,
    ancestor_baseline,
    canonical_nodeid,
    read_baseline,
    write_step_summary,
)

_EXEMPTIONS: pytest.StashKey[set[str]] = pytest.StashKey()
_OBSERVATIONS: pytest.StashKey[DurationObservations] = pytest.StashKey()


def budget_failure(nodeid: str, duration: float, exemptions: set[str]) -> str | None:
    nodeid = canonical_nodeid(nodeid)
    if duration > BUDGET_SECONDS and nodeid not in exemptions:
        return f"Call phase {duration:.3f}s exceeds the {BUDGET_SECONDS}s test budget: {nodeid}"
    return None


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
        if config.getoption("test_time_baseline") is None:
            baseline = ancestor_baseline(config.rootpath) or baseline
        exemptions = set(baseline["durations"])
    except (ValueError, OSError) as error:
        raise pytest.UsageError(f"Cannot load required test-time baseline: {error}") from error
    config.stash[_EXEMPTIONS] = exemptions
    recorder = DurationObservations(exemptions)
    config.stash[_OBSERVATIONS] = recorder
    config.pluginmanager.register(recorder)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    report = yield
    if _EXEMPTIONS not in item.config.stash:
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
        failure = budget_failure(report.nodeid, report.duration, item.config.stash[_EXEMPTIONS])
        if failure:
            report.outcome = "failed"
            report.longrepr = str(report.longrepr) + "\n" + failure if report.longrepr else failure
    return report


class DurationObservations:
    def __init__(self, exemptions: set[str]) -> None:
        self.exemptions = exemptions
        self.calls: dict[str, float] = {}
        self.collected: dict[str, list[str]] = {}
        self.sent: set[str] = set()

    @pytest.hookimpl(wrapper=True, tryfirst=True)
    def pytest_collection_modifyitems(
        self, config: pytest.Config, items: list[pytest.Item]
    ) -> Generator[None]:
        files = {nodeid.split("::", 1)[0] for nodeid in self.exemptions}
        if not any("::" in argument for argument in config.args):
            for item in items:
                nodeid = canonical_nodeid(item.nodeid)
                file = nodeid.split("::", 1)[0]
                if file in files:
                    self.collected.setdefault(file, []).append(nodeid)
        return (yield)

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        for name, value in report.user_properties:
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
            if seconds > BUDGET_SECONDS or nodeid in self.exemptions:
                terminalreporter.write_line(f"{seconds:.6f}s call {nodeid}")
        stale = [
            f"{nodeid}: maximum call {seconds:.6f}s"
            for nodeid, seconds in sorted(self.calls.items())
            if nodeid in self.exemptions and seconds < PRUNE_SECONDS
        ]
        stale.extend(
            f"{nodeid}: unmatched"
            for nodeid in sorted(self.exemptions)
            if nodeid.split("::", 1)[0] in self.collected
            and nodeid not in self.collected[nodeid.split("::", 1)[0]]
        )
        write_step_summary(
            "Duration exemption candidates; regenerate using at least two CI runs:", stale
        )
