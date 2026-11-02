"""Call-phase budget for the pytest PR shards."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pytest
from _pytest.terminal import TerminalReporter

from curie_test_support.test_ratchets import (
    BASELINE,
    BUDGET_SECONDS,
    canonical_nodeid,
    read_baseline,
)

_EXEMPTIONS: pytest.StashKey[set[str]] = pytest.StashKey()


def budget_failure(nodeid: str, duration: float, exemptions: set[str]) -> str | None:
    nodeid = canonical_nodeid(nodeid)
    if duration > BUDGET_SECONDS and nodeid not in exemptions:
        return f"Call phase {duration:.3f}s exceeds the {BUDGET_SECONDS}s test budget: {nodeid}"
    if duration <= BUDGET_SECONDS and nodeid in exemptions:
        return (
            f"Duration baseline is stale for {nodeid}; regenerate with "
            "curie dev test-ratchets --regenerate --durations <artifact>."
        )
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
    config.pluginmanager.register(DurationObservations())
    try:
        config.stash[_EXEMPTIONS] = set(read_baseline(path)["durations"])
    except (ValueError, OSError) as error:
        raise pytest.UsageError(f"Cannot load required test-time baseline: {error}") from error


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> Generator[None]:
    if _EXEMPTIONS in config.stash:
        collected = {canonical_nodeid(item.nodeid) for item in items}
        files = {nodeid.split("::", 1)[0] for nodeid in collected}
        partial_files = {argument.split("::", 1)[0] for argument in config.args if "::" in argument}
        missing = sorted(
            nodeid
            for nodeid in config.stash[_EXEMPTIONS]
            if nodeid.split("::", 1)[0] in files - partial_files and nodeid not in collected
        )
        if missing:
            raise pytest.UsageError(
                "Duration baseline has stale collected identities: " + ", ".join(missing)
            )
    return (yield)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    report = yield
    if (
        report.when == "call"
        and _EXEMPTIONS in item.config.stash
        and (not report.skipped or report.duration > BUDGET_SECONDS)
    ):
        failure = budget_failure(report.nodeid, report.duration, item.config.stash[_EXEMPTIONS])
        if failure:
            report.outcome = "failed"
            report.longrepr = str(report.longrepr) + "\n" + failure if report.longrepr else failure
    return report


class DurationObservations:
    def __init__(self) -> None:
        self.calls: dict[str, float] = {}

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if report.when == "call":
            nodeid = canonical_nodeid(report.nodeid)
            self.calls[nodeid] = max(report.duration, self.calls.get(nodeid, 0))

    def pytest_terminal_summary(self, terminalreporter: TerminalReporter) -> None:
        slow = {
            nodeid: seconds for nodeid, seconds in self.calls.items() if seconds > BUDGET_SECONDS
        }
        terminalreporter.section(f"test-time duration observations: {len(slow)} over budget")
        for nodeid, seconds in sorted(slow.items()):
            terminalreporter.write_line(f"{seconds:.6f}s call {nodeid}")
