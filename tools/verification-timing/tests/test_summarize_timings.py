"""Executable contract for the offline verification-timing summarizer.

The whole point of this tool is that it never guesses a timing. A phase that was
not measured stays unmeasured all the way to the output: it is never zero, never
interpolated, never borrowed from the neighbouring row or the neighbouring phase.
These tests are written to fail loudly the moment a plausible-looking default
sneaks in.
"""

from __future__ import annotations

import importlib.util
import json
import random
import sys
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SUMMARIZER = REPO_ROOT / "tools" / "verification-timing" / "summarize_timings.py"

COMMIT_A = "a" * 40
COMMIT_B = "b" * 40
COMMAND = "uv run pytest tests/test_example.py"
OTHER_COMMAND = "uv run pytest tests/test_other.py"


def _module() -> ModuleType:
    specification = importlib.util.spec_from_file_location("curie_summarize_timings", SUMMARIZER)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules["curie_summarize_timings"] = module
    specification.loader.exec_module(module)
    return module


def _entry(**overrides: object) -> dict:
    entry: dict = {
        "tier": "skill",
        "criterion": "synthetic criterion",
        "command": COMMAND,
        "commit": COMMIT_A,
        "mode": "fake",
        "outcome": "pass",
        "observed": "OBSERVED_SENTINEL",
        "negative": "NEGATIVE_SENTINEL",
        "teardown": "TEARDOWN_SENTINEL",
        "blocker": "BLOCKER_SENTINEL",
    }
    entry.update(overrides)
    return entry


def _state(tmp_path: Path, name: str, entries: list[dict], *, ticket: str = "1234") -> Path:
    path = tmp_path / f"{name}.state.json"
    path.write_text(
        json.dumps(
            {
                "branch": f"task/{name}",
                "ticket": ticket,
                "commit": COMMIT_A,
                "e2e": {"evidence": entries},
            }
        )
    )
    return path


def _phase(summary: dict, index: int, phase: str) -> dict:
    return summary["rows"][index]["timings"][phase]


def test_phases_tuple_is_exact_and_ordered() -> None:
    module = _module()
    assert module.PHASES == ("startup", "tests", "cleanup", "retries", "external_wait")
    assert isinstance(module.PHASES, tuple)


def test_timing_record_error_is_a_value_error() -> None:
    module = _module()
    assert issubclass(module.TimingRecordError, ValueError)


def test_unknown_phase_is_none_never_zero_and_never_borrowed(tmp_path: Path) -> None:
    """Rule 1: observed-only. An unmeasured phase must not acquire a number."""
    module = _module()
    path = _state(
        tmp_path,
        "unknown",
        [
            _entry(timings={"startup": {"seconds": 12.5}, "tests": {"seconds": 44.0}}),
            _entry(
                commit=COMMIT_B,
                timings={"startup": None, "tests": {"seconds": None}},
            ),
        ],
    )
    summary = module.summarize(module.load_records([path]))

    for phase in ("startup", "tests"):
        cell = _phase(summary, 1, phase)
        assert cell["seconds"] is None, f"{phase} was filled in"
        assert cell["seconds"] != 0.0
        assert cell["provenance"] == "unknown"
    # Absent keys behave exactly like explicit nulls, and never inherit a sibling.
    for phase in ("cleanup", "retries", "external_wait"):
        assert _phase(summary, 0, phase)["seconds"] is None
        assert _phase(summary, 0, phase)["provenance"] == "unknown"


def test_provenance_distinguishes_timestamps_seconds_and_unknown(tmp_path: Path) -> None:
    module = _module()
    path = _state(
        tmp_path,
        "provenance",
        [
            _entry(
                timings={
                    "startup": {
                        "started_at": "2026-09-10T10:00:00Z",
                        "completed_at": "2026-09-10T10:00:30Z",
                    },
                    "tests": {"seconds": 12.5},
                }
            )
        ],
    )
    summary = module.summarize(module.load_records([path]))

    assert _phase(summary, 0, "startup") == {"seconds": 30.0, "provenance": "timestamps"}
    assert _phase(summary, 0, "tests") == {"seconds": 12.5, "provenance": "seconds"}
    assert _phase(summary, 0, "cleanup")["provenance"] == "unknown"


def test_startup_never_contributes_to_the_tests_total(tmp_path: Path) -> None:
    """Rule 3: the two phases stay separate buckets, both directions."""
    module = _module()
    path = _state(
        tmp_path,
        "separation",
        [_entry(timings={"startup": {"seconds": 100.0}, "tests": {"seconds": 7.0}})],
    )
    summary = module.summarize(module.load_records([path]))
    totals = summary["phase_totals"]

    assert tuple(totals) == module.PHASES
    assert totals["startup"]["total_seconds"] == 100.0
    assert totals["tests"]["total_seconds"] == 7.0
    assert totals["tests"]["total_seconds"] != 107.0
    assert totals["startup"]["observed_count"] == 1
    assert totals["tests"]["observed_count"] == 1


def test_phase_totals_are_none_when_nothing_was_observed(tmp_path: Path) -> None:
    module = _module()
    path = _state(tmp_path, "empty", [_entry(timings={"startup": {"seconds": 5.0}})])
    totals = module.summarize(module.load_records([path]))["phase_totals"]["cleanup"]

    assert totals["observed_count"] == 0
    assert totals["unknown_count"] == 1
    assert totals["total_seconds"] is None
    assert totals["median_seconds"] is None
    assert totals["p90_seconds"] is None


def test_median_and_p90_use_the_observed_subset_only(tmp_path: Path) -> None:
    """Rule 4: eleven observations 10..110 plus unknowns that must not drag it down."""
    module = _module()
    observed = [
        _entry(
            commit=COMMIT_A,
            command=f"{COMMAND}::{index}",
            timings={"tests": {"seconds": float(value)}},
        )
        for index, value in enumerate(range(10, 120, 10))
    ]
    unknown = [
        _entry(commit=COMMIT_A, command=f"{COMMAND}::u{index}", timings={"tests": None})
        for index in range(5)
    ]
    path = _state(tmp_path, "stats", observed + unknown)
    totals = module.summarize(module.load_records([path]))["phase_totals"]["tests"]

    assert totals["observed_count"] == 11
    assert totals["unknown_count"] == 5
    assert totals["total_seconds"] == pytest.approx(660.0)
    assert totals["median_seconds"] == pytest.approx(60.0)
    assert totals["p90_seconds"] == pytest.approx(100.0)


def test_retry_counts_aggregate_and_stay_separate_from_duration(tmp_path: Path) -> None:
    module = _module()
    path = _state(
        tmp_path,
        "retries",
        [
            _entry(command=f"{COMMAND}::1", timings={"retries": {"count": 2, "seconds": 30.0}}),
            _entry(command=f"{COMMAND}::2", timings={"retries": {"count": 3, "seconds": None}}),
        ],
    )
    summary = module.summarize(module.load_records([path]))
    totals = summary["phase_totals"]["retries"]

    assert summary["retry_total_count"] == 5
    assert totals["observed_count"] == 1
    assert totals["unknown_count"] == 1
    assert totals["total_seconds"] == pytest.approx(30.0)
    assert _phase(summary, 1, "retries")["seconds"] is None


def test_retry_total_count_is_none_when_no_row_declared_one(tmp_path: Path) -> None:
    module = _module()
    path = _state(tmp_path, "noretries", [_entry(timings={"tests": {"seconds": 1.0}})])
    summary = module.summarize(module.load_records([path]))

    assert summary["retry_total_count"] is None
    assert summary["retry_total_count"] != 0


def test_summary_is_byte_identical_regardless_of_input_order(tmp_path: Path) -> None:
    module = _module()
    paths = [
        _state(
            tmp_path,
            f"det{index}",
            [
                _entry(
                    commit=COMMIT_A,
                    command=f"{COMMAND}::{index}",
                    timings={"tests": {"seconds": float(index)}},
                )
            ],
        )
        for index in range(6)
    ]
    baseline = json.dumps(module.summarize(module.load_records(paths)), sort_keys=True)

    shuffled = list(paths)
    random.Random(1234).shuffle(shuffled)
    assert shuffled != paths
    assert json.dumps(module.summarize(module.load_records(shuffled)), sort_keys=True) == baseline


def test_duplicates_are_grouped_but_never_merged_away(tmp_path: Path) -> None:
    module = _module()
    first = _state(tmp_path, "dup1", [_entry(timings={"tests": {"seconds": 1.0}})])
    second = _state(tmp_path, "dup2", [_entry(timings={"tests": {"seconds": 2.0}})])
    summary = module.summarize(module.load_records([first, second]))

    assert len(summary["duplicate_groups"]) == 1
    group = summary["duplicate_groups"][0]
    sources = json.dumps(group)
    assert first.name in sources and second.name in sources
    assert len(summary["rows"]) == 2
    assert {row["timings"]["tests"]["seconds"] for row in summary["rows"]} == {1.0, 2.0}


def test_rows_differing_only_by_commit_are_not_duplicates(tmp_path: Path) -> None:
    module = _module()
    first = _state(
        tmp_path, "uniq1", [_entry(commit=COMMIT_A, timings={"tests": {"seconds": 1.0}})]
    )
    second = _state(
        tmp_path, "uniq2", [_entry(commit=COMMIT_B, timings={"tests": {"seconds": 1.0}})]
    )
    summary = module.summarize(module.load_records([first, second]))

    assert summary["duplicate_groups"] == []
    assert len(summary["rows"]) == 2


@pytest.mark.parametrize(
    "timings",
    [
        pytest.param({"tests": {"seconds": -1.0}}, id="negative-seconds"),
        pytest.param(
            {
                "tests": {
                    "started_at": "2026-09-10T10:00:30Z",
                    "completed_at": "2026-09-10T10:00:00Z",
                }
            },
            id="completed-before-started",
        ),
        pytest.param({"tests": {"seconds": "fast"}}, id="non-numeric-seconds"),
        pytest.param({"retries": {"count": -1}}, id="negative-retry-count"),
        pytest.param({"warmup": {"seconds": 1.0}}, id="unknown-phase-key"),
        pytest.param(
            {
                "tests": {
                    "seconds": 90.0,
                    "started_at": "2026-09-10T10:00:00Z",
                    "completed_at": "2026-09-10T10:00:30Z",
                }
            },
            id="seconds-disagree-with-timestamps",
        ),
    ],
)
def test_malformed_records_raise_naming_the_source_path(tmp_path: Path, timings: dict) -> None:
    module = _module()
    path = _state(tmp_path, "bad", [_entry(timings=timings)])

    with pytest.raises(module.TimingRecordError) as excinfo:
        module.summarize(module.load_records([path]))
    assert str(path) in str(excinfo.value)


def test_seconds_agreeing_with_timestamps_within_one_second_is_accepted(tmp_path: Path) -> None:
    module = _module()
    path = _state(
        tmp_path,
        "agree",
        [
            _entry(
                timings={
                    "tests": {
                        "seconds": 30.4,
                        "started_at": "2026-09-10T10:00:00Z",
                        "completed_at": "2026-09-10T10:00:30Z",
                    }
                }
            )
        ],
    )
    cell = _phase(module.summarize(module.load_records([path])), 0, "tests")
    assert cell["seconds"] is not None
    assert cell["seconds"] == pytest.approx(30.0, abs=1.0)


def test_summary_carries_provenance_but_no_free_text(tmp_path: Path) -> None:
    module = _module()
    path = _state(tmp_path, "privacy", [_entry(timings={"tests": {"seconds": 3.0}})])
    rendered = json.dumps(module.summarize(module.load_records([path])))

    assert COMMIT_A in rendered
    assert "skill" in rendered
    assert COMMAND in rendered
    for sentinel in (
        "OBSERVED_SENTINEL",
        "NEGATIVE_SENTINEL",
        "TEARDOWN_SENTINEL",
        "BLOCKER_SENTINEL",
    ):
        assert sentinel not in rendered


def test_state_files_without_timings_load_and_are_all_unknown(tmp_path: Path) -> None:
    module = _module()
    path = _state(tmp_path, "legacy", [_entry(), _entry(commit=COMMIT_B)])
    summary = module.summarize(module.load_records([path]))

    assert len(summary["rows"]) == 2
    for row in summary["rows"]:
        for phase in module.PHASES:
            assert row["timings"][phase] == {"seconds": None, "provenance": "unknown"}
    assert all(summary["phase_totals"][phase]["observed_count"] == 0 for phase in module.PHASES)


def test_cli_summarizes_a_directory_and_exits_zero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    module = _module()
    _state(tmp_path, "cli1", [_entry(command=f"{COMMAND}::1", timings={"tests": {"seconds": 1.0}})])
    _state(tmp_path, "cli2", [_entry(command=OTHER_COMMAND, timings={"tests": {"seconds": 2.0}})])

    assert module.main([str(tmp_path)]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert len(printed["rows"]) == 2


def test_cli_exits_two_and_prints_no_summary_on_a_malformed_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    module = _module()
    _state(tmp_path, "clibad", [_entry(timings={"tests": {"seconds": -5.0}})])

    assert module.main([str(tmp_path)]) == 2
    captured = capsys.readouterr()
    assert captured.out.strip() == ""
