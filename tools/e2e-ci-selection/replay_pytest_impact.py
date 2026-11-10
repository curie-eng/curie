"""Replay the pull request pytest selection over merged pull requests.

The replay answers one question for a pull request body: with impact selection
on, how long would the Python pytest shards take on a typical pull request?

`build-input` turns a list of merged pull requests (number, changed files, and
optionally the merge commit's two parents as `base` and `head`) plus the JUnit
XML files that a full CI run wrote per shard into one JSON input. Each test
file in that input carries its test count and the summed time of its test
cases; each shard carries its suite time and its files.

`run` loads the real selection registry and builds the impact graph for the
current tree once. For every pull request it computes what the selector in
`select_tiers.py` would output on a pull_request event: whether pytest runs at
all, the scope (full, cli or impact), the targets, the conditional deselect
list and the shard plan (shard count, storage shards, full shards). It calls
the selector's own helpers in the order `select_tiers._run` does. Revision mode
(ci.yaml hunks and registry section comparison) is used when both parent
commits exist locally; otherwise path mode, which treats any ci.yaml or
registry change as touching everything.

The estimate model. The selected tests are the input's test files inside the
selection, minus the registry's excluded paths and the deselect list. They are
grouped as the root conftest groups them (the shared live state group, the
protected runner group, otherwise one group per file) and assigned to shards
with the root conftest's own `assign_shards`, with service tiers from
`pytest_impact.tiers_for` and the storage and full limits exactly as a pull
request passes them. A shard's time is a collection overhead scaled by the
selected share of all tests, plus the makespan of a longest processing time
first schedule of that shard's groups on four workers. A pull request's
estimate is its slowest active shard, or zero when pytest does not run. The
collection overhead is calibrated from the input: for each recorded shard it is
the suite time minus the same schedule's makespan for that shard's files, and
the median over shards is used.

Limits. Test times come from one recorded run of the current suite, not from
the tree each pull request had, so new or deleted tests and timing noise are
not modelled. xdist's loadgroup scheduler is approximated by the longest
processing time schedule, which ignores per test dispatch order and worker
start cost. Service boot time before pytest starts (the dev stack, Langfuse)
and the database migration step are not part of the estimate, so a lower
service tier's saving appears only as the shard count it allows. A selected
file absent from the input counts as zero tests and zero seconds. The root
conftest is imported by path, so the run needs pytest importable (run it with
`uv run`).
"""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import json
import math
import os
import re
import statistics
import subprocess
import sys
import xml.etree.ElementTree as ElementTree
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any, cast

TOOLS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TOOLS_DIR.parents[1]
WORKERS = 4
BASELINE_SHARDS = 6
SUBCOMMANDS = ("build-input", "run")
_TRAILING_NUMBER = re.compile(r"(\d+)$")


def _load_module(name: str, path: Path) -> ModuleType:
    loaded = sys.modules.get(name)
    if loaded is not None:
        return loaded
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    # Dataclasses resolve their module through sys.modules while the class body runs.
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return module


def _under(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(f"{prefix}/")


def _under_any(path: str, prefixes: Iterable[str]) -> bool:
    return any(_under(path, prefix) for prefix in prefixes)


def _within(path: str, roots: frozenset[str]) -> bool:
    """Is `path` one of `roots` or under one of them (roots may be files or directories)?"""
    if path in roots:
        return True
    parts = path.split("/")
    return any("/".join(parts[:index]) in roots for index in range(1, len(parts)))


# build-input


def _shard_number(path: Path) -> int:
    for name in (path.stem, path.parent.name):
        match = _TRAILING_NUMBER.search(name)
        if match is not None:
            return int(match.group(1))
    raise SystemExit(f"cannot tell the shard number of {path}: name it junit-N.xml")


class _ClassnameResolver:
    """Map an xunit classname such as `apps.api.tests.test_x.TestY` to its file."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.cache: dict[str, str | None] = {}

    def __call__(self, classname: str) -> str | None:
        if classname not in self.cache:
            parts = classname.split(".")
            found = None
            for index in range(len(parts), 0, -1):
                candidate = "/".join(parts[:index]) + ".py"
                if (self.root / candidate).is_file():
                    found = candidate
                    break
            self.cache[classname] = found
        return self.cache[classname]


def _read_junit(
    path: Path, resolve: _ClassnameResolver
) -> tuple[float, dict[str, tuple[int, float]], int]:
    """Suite seconds, per file (tests, seconds), and the count of unplaced cases."""
    root = ElementTree.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
    suite_seconds = sum(float(suite.get("time") or 0.0) for suite in suites)
    files: dict[str, tuple[int, float]] = {}
    unplaced = 0
    for case in root.iter("testcase"):
        test_file = case.get("file") or resolve(case.get("classname") or "")
        if not test_file:
            unplaced += 1
            continue
        test_file = test_file.removeprefix("./")
        tests, seconds = files.get(test_file, (0, 0.0))
        files[test_file] = (tests + 1, seconds + float(case.get("time") or 0.0))
    return suite_seconds, files, unplaced


def _validate_prs(document: object) -> list[dict[str, Any]]:
    if not isinstance(document, list):
        raise SystemExit("the pull request JSON must be a list")
    prs: list[dict[str, Any]] = []
    for entry in document:
        if (
            not isinstance(entry, dict)
            or type(entry.get("number")) is not int
            or not isinstance(entry.get("files"), list)
            or any(not isinstance(item, str) for item in entry["files"])
        ):
            raise SystemExit("each pull request needs an integer number and a files list of paths")
        for key in ("base", "head"):
            if entry.get(key) is not None and not isinstance(entry[key], str):
                raise SystemExit(f"pull request {entry['number']}: {key} must be a commit id")
        prs.append(cast(dict[str, Any], entry))
    return prs


def build_input(prs_path: Path, junit_dir: Path, out: Path) -> int:
    prs = _validate_prs(json.loads(prs_path.read_text(encoding="utf-8")))
    xml_files = sorted(junit_dir.rglob("*.xml"))
    if not xml_files:
        raise SystemExit(f"no JUnit XML files under {junit_dir}")
    resolve = _ClassnameResolver(REPO_ROOT)
    totals: dict[str, list[float]] = {}
    shards: dict[int, dict[str, Any]] = {}
    unplaced = 0
    for xml_file in xml_files:
        number = _shard_number(xml_file)
        suite_seconds, files, missing = _read_junit(xml_file, resolve)
        unplaced += missing
        shard = shards.setdefault(number, {"shard": number, "suite_seconds": 0.0, "files": set()})
        shard["suite_seconds"] += suite_seconds
        shard["files"].update(files)
        for test_file, (tests, seconds) in files.items():
            total = totals.setdefault(test_file, [0, 0.0])
            total[0] += tests
            total[1] += seconds
    document = {
        "prs": prs,
        "files": {
            test_file: {"tests": int(tests), "seconds": round(seconds, 3)}
            for test_file, (tests, seconds) in sorted(totals.items())
        },
        "shards": [
            {
                "shard": number,
                "suite_seconds": round(shards[number]["suite_seconds"], 3),
                "files": sorted(shards[number]["files"]),
            }
            for number in sorted(shards)
        ],
    }
    out.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(
        f"wrote {out}: {len(prs)} pull requests, {len(totals)} test files, "
        f"{sum(int(t[0]) for t in totals.values())} tests, {len(shards)} shards"
        + (f", {unplaced} test cases without a file skipped" if unplaced else "")
    )
    return 0


# run: selection glue


@dataclass(frozen=True)
class PrSelection:
    """What the selector outputs for one pull request's pytest."""

    pytest_runs: bool
    mode: str
    scope: str
    reason: str
    targets: tuple[str, ...]
    deselect: tuple[str, ...]
    shards: int
    storage_shards: int
    full_shards: int


class Selector:
    """The pull request pytest selection, with the registry and graph loaded once."""

    def __init__(self) -> None:
        # select_tiers runs git and reads ci.yaml relative to the working directory.
        os.chdir(REPO_ROOT)
        self.tiers = _load_module("e2e_ci_selection_select_tiers", TOOLS_DIR / "select_tiers.py")
        self.registry = self.tiers._load_registry(REPO_ROOT / self.tiers.REGISTRY_PATH)
        if self.registry.pytest_impact is None:
            raise SystemExit("the registry has no pytest_impact section; nothing to replay")
        self.impact: ModuleType = self.tiers._impact()
        self.config = self.registry.pytest_impact
        self.graph = self.impact.ImpactGraph.build(REPO_ROOT)
        self.cli_files: list[str] = self.tiers._cli_list_files(self.registry)
        self._commits: dict[str, bool] = {}

    def has_commit(self, revision: str | None) -> bool:
        if not revision:
            return False
        if revision not in self._commits:
            completed = subprocess.run(
                ["git", "cat-file", "-e", f"{revision}^{{commit}}"],
                cwd=REPO_ROOT,
                capture_output=True,
                check=False,
            )
            self._commits[revision] = completed.returncode == 0
        return self._commits[revision]

    @contextlib.contextmanager
    def _reuse_graph(self) -> Iterator[None]:
        """Let select_tiers._pytest_plan use the graph built once, not rebuild it per PR."""
        graph_class = self.impact.ImpactGraph
        original = graph_class.__dict__["build"]
        graph = self.graph

        def build(cls: type, repo_root: Path, tracked: Sequence[str] | None = None) -> Any:
            if tracked is None and Path(repo_root).resolve() == graph.root:
                return graph
            return original.__func__(cls, repo_root, tracked)

        graph_class.build = classmethod(build)
        try:
            yield
        finally:
            graph_class.build = original

    def select(self, pr: Mapping[str, Any]) -> PrSelection:
        st = self.tiers
        registry = self.registry
        paths: list[str] = list(pr["files"])
        base, head = pr.get("base"), pr.get("head")
        revision = self.has_commit(base) and self.has_commit(head)
        workflow_jobs_changed: frozenset[str] | None = None
        registry_pytest_changed: bool | None = None
        mode = "path"
        if revision:
            mode = "revision"
            workflow_jobs_changed = frozenset()
            if st.WORKFLOW_PATH in paths:
                try:
                    _, workflow_jobs_changed = st._workflow_changes(base, head)
                except (st.RegistryError, subprocess.CalledProcessError):
                    # The selector would fail this run; path mode is the closest replay.
                    workflow_jobs_changed = None
                    mode = "revision (ci.yaml unreadable, path facts)"
            registry_pytest_changed = st.REGISTRY_PATH in paths and st._registry_pytest_changed(
                base, head
            )

        with self._reuse_graph():
            pytest_needed = st._pytest_needed(
                registry,
                paths,
                pull_request=True,
                workflow_jobs_changed=workflow_jobs_changed,
                registry_pytest_changed=registry_pytest_changed,
            )
        version_only = st._is_version_only(paths, head if revision else None)
        if version_only:
            pytest_needed = False

        pytest_deselect: list[str] = []
        if (
            paths
            and pytest_needed
            and all(
                path in registry.exact
                or path in registry.ignored_exact
                or any(
                    st._matches_prefix(path, prefix)
                    for prefix in (*registry.prefixes, *registry.ignored_prefixes)
                )
                for path in paths
            )
        ):
            pytest_deselect = sorted(
                test_file
                for test_file, inputs in registry.conditional_pytest_files.items()
                if not any(st._matches_prefix(path, prefix) for path in paths for prefix in inputs)
            )

        pytest_scope = "full"
        if (
            paths
            and pytest_needed
            and all(st._matches_pytest_scope(registry.pytest_scopes["cli"], path) for path in paths)
        ):
            pytest_scope = "cli"

        if not pytest_needed:
            reason = "version only" if version_only else "pytest not selected"
            return PrSelection(False, mode, "none", reason, (), (), 0, 0, 0)
        with self._reuse_graph():
            plan = st._pytest_plan(
                registry,
                paths,
                pytest_scope,
                pull_request=True,
                pytest_needed=pytest_needed,
                workflow_jobs_changed=workflow_jobs_changed,
                registry_pytest_changed=registry_pytest_changed,
            )
        return PrSelection(
            True,
            mode,
            plan.scope,
            plan.reason,
            tuple(plan.targets),
            tuple(pytest_deselect),
            plan.shards,
            plan.storage_shards,
            plan.full_shards,
        )


# run: estimate


@dataclass(frozen=True)
class FileStat:
    tests: int
    seconds: float


def _lpt_makespan(durations: Iterable[float], workers: int = WORKERS) -> float:
    loads = [0.0] * workers
    for duration in sorted(durations, reverse=True):
        index = min(range(workers), key=lambda worker: (loads[worker], worker))
        loads[index] += duration
    return max(loads)


class Estimator:
    def __init__(
        self,
        files: Mapping[str, FileStat],
        excluded: Sequence[str],
        file_tiers: Mapping[str, int],
    ) -> None:
        self.conftest = _load_module("curie_replay_root_conftest", REPO_ROOT / "conftest.py")
        self.files = dict(files)
        self.file_tiers = dict(file_tiers)
        self.full_set = sorted(path for path in files if not _under_any(path, excluded))
        self.all_tests = sum(files[path].tests for path in self.full_set)
        self.overhead = 0.0

    def group_of(self, path: str) -> str:
        conftest = self.conftest
        group = (
            conftest.SHARED_LIVE_STATE_GROUP if path in conftest.SHARED_LIVE_STATE_FILES else path
        )
        if path in conftest.PROTECTED_RUNNER_FILES:
            group = conftest.PROTECTED_RUNNER_GROUP
        return str(group)

    def groups(self, paths: Iterable[str]) -> dict[str, list[str]]:
        grouped: dict[str, list[str]] = {}
        for path in paths:
            grouped.setdefault(self.group_of(path), []).append(path)
        return grouped

    def group_seconds(self, members: Iterable[str]) -> float:
        return sum(self.files[path].seconds for path in members if path in self.files)

    def calibrate(self, shards: Sequence[Mapping[str, Any]]) -> list[float]:
        overheads = []
        for shard in shards:
            grouped = self.groups(path for path in shard["files"] if path in self.files)
            makespan = _lpt_makespan(self.group_seconds(members) for members in grouped.values())
            overheads.append(max(0.0, float(shard["suite_seconds"]) - makespan))
        self.overhead = statistics.median(overheads) if overheads else 0.0
        return overheads

    def estimate(
        self, selected: Sequence[str], shards: int, storage: int, full: int, tiered: bool
    ) -> tuple[float, list[float], str]:
        """Slowest shard seconds, every shard's seconds, and a placement error if any."""
        grouped = self.groups(selected)
        sizes = {
            group: sum(self.files[p].tests for p in members) for group, members in grouped.items()
        }
        error = ""
        if tiered and (storage, full) != (shards, shards):
            group_tiers = {
                group: max((self.file_tiers.get(path, 0) for path in members), default=0)
                for group, members in grouped.items()
            }
            try:
                owner = self.conftest.assign_shards(
                    sizes, shards, group_tiers=group_tiers, tier_limits=(storage, full)
                )
            except Exception as exc:  # noqa: BLE001 - pytest.UsageError; the CI run would fail
                error = str(exc)
                owner = self.conftest.assign_shards(sizes, shards)
        else:
            owner = self.conftest.assign_shards(sizes, shards)
        selected_tests = sum(sizes.values())
        collection = self.overhead * selected_tests / self.all_tests if self.all_tests else 0.0
        per_shard: list[list[float]] = [[] for _ in range(shards)]
        for group, members in grouped.items():
            per_shard[owner[group] - 1].append(self.group_seconds(members))
        times = [collection + _lpt_makespan(durations) for durations in per_shard]
        return max(times, default=0.0), times, error


@dataclass
class Row:
    number: int
    mode: str
    scope: str
    reason: str
    selected_tests: int
    all_tests: int
    fraction: float
    shards: int
    storage_shards: int
    full_shards: int
    minutes: float
    pytest_runs: bool
    selected_files: int
    targets_without_timing: int
    shard_minutes: list[float] = field(default_factory=list)
    placement_error: str = ""


def _percentile(values: Sequence[float], share: float) -> float:
    """Linear interpolation between closest ranks (numpy's default)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * share
    lower = math.floor(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _summary(rows: Sequence[Row]) -> dict[str, dict[str, float]]:
    def stats(subset: Sequence[Row]) -> dict[str, float]:
        fractions = [row.fraction for row in subset]
        minutes = [row.minutes for row in subset]
        return {
            "count": len(subset),
            "fraction_p50": _percentile(fractions, 0.5),
            "fraction_p90": _percentile(fractions, 0.9),
            "minutes_p50": _percentile(minutes, 0.5),
            "minutes_p90": _percentile(minutes, 0.9),
        }

    return {
        "all": stats(rows),
        "pytest_runs": stats([row for row in rows if row.pytest_runs]),
    }


def _replay(document: Mapping[str, Any]) -> dict[str, Any]:
    prs = _validate_prs(document.get("prs"))
    raw_files = document.get("files")
    if not isinstance(raw_files, dict) or not raw_files:
        raise SystemExit("the input has no files timing map")
    files = {
        str(path): FileStat(int(stat["tests"]), float(stat["seconds"]))
        for path, stat in raw_files.items()
    }
    selector = Selector()
    config = selector.config
    tracked = [path for path in sorted(files) if path in selector.graph.tracked]
    file_tiers: dict[str, int] = selector.impact.tiers_for(REPO_ROOT, tracked)
    estimator = Estimator(files, config.excluded, file_tiers)
    overheads = estimator.calibrate(document.get("shards") or [])
    full_set = estimator.full_set
    cli_roots = frozenset(selector.cli_files)

    rows: list[Row] = []
    for pr in prs:
        choice = selector.select(pr)
        if not choice.pytest_runs:
            rows.append(
                Row(
                    pr["number"],
                    choice.mode,
                    choice.scope,
                    choice.reason,
                    0,
                    estimator.all_tests,
                    0.0,
                    0,
                    0,
                    0,
                    0.0,
                    False,
                    0,
                    0,
                )
            )
            continue
        if choice.scope == "impact":
            roots = frozenset(choice.targets)
        elif choice.scope == "cli":
            roots = cli_roots
        else:
            roots = frozenset()
        dropped = frozenset(choice.deselect)
        selected = [
            path
            for path in full_set
            if (choice.scope == "full" or _within(path, roots)) and not _within(path, dropped)
        ]
        missing = sum(1 for target in roots if target.endswith(".py") and target not in files)
        seconds, shard_seconds, error = estimator.estimate(
            selected, choice.shards, choice.storage_shards, choice.full_shards, tiered=True
        )
        selected_tests = sum(files[path].tests for path in selected)
        rows.append(
            Row(
                number=pr["number"],
                mode=choice.mode,
                scope=choice.scope,
                reason=choice.reason,
                selected_tests=selected_tests,
                all_tests=estimator.all_tests,
                fraction=selected_tests / estimator.all_tests if estimator.all_tests else 0.0,
                shards=choice.shards,
                storage_shards=choice.storage_shards,
                full_shards=choice.full_shards,
                minutes=seconds / 60,
                pytest_runs=True,
                selected_files=len(selected),
                targets_without_timing=missing,
                shard_minutes=[round(value / 60, 2) for value in shard_seconds],
                placement_error=error,
            )
        )

    baseline_seconds, baseline_shards, _ = estimator.estimate(
        full_set, BASELINE_SHARDS, BASELINE_SHARDS, BASELINE_SHARDS, tiered=False
    )
    return {
        "overhead_seconds": estimator.overhead,
        "shard_overheads_seconds": [round(value, 1) for value in overheads],
        "all_tests": estimator.all_tests,
        "baseline_minutes": baseline_seconds / 60,
        "baseline_shard_minutes": [round(value / 60, 2) for value in baseline_shards],
        "summary": _summary(rows),
        "rows": rows,
    }


def _summary_text(result: Mapping[str, Any]) -> str:
    summary = result["summary"]
    lines = [
        f"collection overhead {result['overhead_seconds']:.1f}s (median of "
        f"{len(result['shard_overheads_seconds'])} shards); full suite on {BASELINE_SHARDS} "
        f"shards {result['baseline_minutes']:.1f} min; {result['all_tests']} tests in the full set",
    ]
    for key, label in (("all", "all PRs"), ("pytest_runs", "PRs running pytest")):
        stats = summary[key]
        lines.append(
            f"{label} ({stats['count']}): selected fraction p50 {stats['fraction_p50']:.2f} "
            f"p90 {stats['fraction_p90']:.2f}; estimated minutes p50 {stats['minutes_p50']:.1f} "
            f"p90 {stats['minutes_p90']:.1f}"
        )
    errors = [row.number for row in result["rows"] if row.placement_error]
    if errors:
        lines.append(
            "tier placement would fail (conftest UsageError) for PRs: "
            + ", ".join(f"#{number}" for number in errors)
        )
    return "\n".join(lines)


def _cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def _markdown(result: Mapping[str, Any]) -> str:
    summary = result["summary"]
    rows: list[Row] = result["rows"]
    every, running = summary["all"], summary["pytest_runs"]
    out = [
        "### Pytest impact replay",
        "",
        f"The selector was replayed over {every['count']} merged pull requests, of which "
        f"{running['count']} would run pytest. Estimated minutes are the slowest active shard's "
        "pytest time: a collection overhead scaled by the selected share of tests plus a longest "
        f"processing time schedule of the shard's xdist groups on {WORKERS} workers. Service "
        "boot and migration time are not included. The collection overhead is "
        f"{result['overhead_seconds']:.0f} seconds, and the full suite on {BASELINE_SHARDS} "
        f"shards estimates at {result['baseline_minutes']:.1f} minutes.",
        "",
        "| Statistic | All PRs | PRs running pytest |",
        "|---|---|---|",
        f"| Selected fraction p50 | {every['fraction_p50']:.2f} | {running['fraction_p50']:.2f} |",
        f"| Selected fraction p90 | {every['fraction_p90']:.2f} | {running['fraction_p90']:.2f} |",
        f"| Estimated minutes p50 | {every['minutes_p50']:.1f} | {running['minutes_p50']:.1f} |",
        f"| Estimated minutes p90 | {every['minutes_p90']:.1f} | {running['minutes_p90']:.1f} |",
        "",
        "| PR | Scope | Reason | Selected tests | All tests | Fraction | Shards (N/storage/full) "
        "| Est. minutes |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        reason = row.reason + (" (tier placement fails)" if row.placement_error else "")
        out.append(
            f"| #{row.number} | {row.scope} | {_cell(reason)} | {row.selected_tests} | "
            f"{row.all_tests} | {row.fraction:.2f} | {row.shards}/{row.storage_shards}/"
            f"{row.full_shards} | {row.minutes:.1f} |"
        )
    return "\n".join(out) + "\n"


def run(
    input_path: Path,
    max_p50_minutes: float | None,
    json_out: Path | None,
    markdown_out: Path | None,
) -> int:
    document = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise SystemExit("the input must be a JSON object")
    result = _replay(document)
    print(_summary_text(result))
    if json_out is not None:
        serial = dict(result, rows=[asdict(row) for row in result["rows"]])
        json_out.write_text(json.dumps(serial, indent=2) + "\n", encoding="utf-8")
    if markdown_out is not None:
        markdown_out.write_text(_markdown(result), encoding="utf-8")
    p50 = float(result["summary"]["all"]["minutes_p50"])
    if max_p50_minutes is not None and p50 > max_p50_minutes:
        print(f"p50 estimate {p50:.1f} min exceeds the {max_p50_minutes:g} min limit")
        return 1
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build-input", help="Combine PR diffs and JUnit timings.")
    build.add_argument("--prs", required=True, type=Path)
    build.add_argument("--junit-dir", required=True, type=Path)
    build.add_argument("--out", required=True, type=Path)
    replay = commands.add_parser("run", help="Replay the selection and estimate pytest time.")
    replay.add_argument("--input", required=True, type=Path)
    replay.add_argument("--max-p50-minutes", type=float)
    replay.add_argument("--json-out", type=Path)
    replay.add_argument("--markdown-out", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] not in (*SUBCOMMANDS, "-h", "--help"):
        arguments.insert(0, "run")
    args = _parser().parse_args(arguments)

    # Paths are taken relative to where the caller ran, before the selector moves to the root.
    def absolute(value: Path | None) -> Path | None:
        return value.resolve() if value is not None else None

    if args.command == "build-input":
        return build_input(args.prs.resolve(), args.junit_dir.resolve(), args.out.resolve())
    return run(
        args.input.resolve(),
        args.max_p50_minutes,
        absolute(args.json_out),
        absolute(args.markdown_out),
    )


if __name__ == "__main__":
    raise SystemExit(main())
