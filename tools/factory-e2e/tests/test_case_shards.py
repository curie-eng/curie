from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from case_shards import assign_case_shards, load_durations


def test_committed_weights_partition_known_unknown_and_stale_cases() -> None:
    weights = load_durations()
    known = list(weights)
    stale = known.pop()
    nodes = [*reversed(known), "tests/example.py::test_unknown"]
    shards = assign_case_shards(nodes, 7, weights)
    assert shards == assign_case_shards(nodes, 7, weights)
    assert shards == assign_case_shards(nodes, 7, dict(reversed(list(weights.items()))))
    flattened = [node for shard in shards for node in shard]
    assert set(flattened) == set(nodes)
    assert len(flattened) == len(nodes)
    assert stale not in flattened
    for shard in shards:
        assert shard == [node for node in nodes if node in shard]


def test_longest_first_unknown_median_and_node_id_ties() -> None:
    nodes = ["small", "unknown", "equal-b", "large", "equal-a"]
    weights = {"small": 1, "large": 9, "equal-a": 5, "equal-b": 5, "stale": 5}
    assert assign_case_shards(nodes, 2, weights) == [
        ["unknown", "large"],
        ["small", "equal-b", "equal-a"],
    ]


def test_colocated_cases_are_assigned_by_summed_weight() -> None:
    module = "tools/factory-e2e/tests/test_c4_admitted_agent.py"
    group = [
        f"{module}::test_continuation_revalidates_current_authority[{change}-{reason}-{phase}]"
        for change, reason in (
            ("removed_binding", "factory_binding_missing"),
            ("binding_tenant", "factory_tenant_mismatch"),
            ("tracker_surface", "factory_surface_mismatch"),
        )
        for phase in ("held", "restarted")
    ] + [f"{module}::test_two_worker_ci_preserves_successor_identity_and_rejects_stale_predecessor"]
    weights = {**dict.fromkeys(group, 2), "heavy": 9, "light": 8, "stale": 1000}
    assert assign_case_shards(["heavy", *group, "light"], 2, weights) == [group, ["heavy", "light"]]
    partial = group[1:]
    assert assign_case_shards(["heavy", *partial, "light"], 2, weights) == [
        partial,
        ["heavy", "light"],
    ]


@pytest.mark.parametrize("data", [{}, {"case": -1}, {"case": "bad"}])
def test_invalid_duration_file_is_refused(tmp_path: Path, data: object) -> None:
    path = tmp_path / "durations.json"
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        load_durations(path)


def test_missing_duration_file_is_refused(tmp_path: Path) -> None:
    with pytest.raises(OSError):
        load_durations(tmp_path / "missing.json")


def test_malformed_duration_file_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "durations.json"
    path.write_text("not json")
    with pytest.raises(ValueError):
        load_durations(path)


def test_unreadable_duration_file_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "durations.json"
    path.write_text('{"case": 1}')
    path.chmod(0)
    try:
        with pytest.raises(OSError):
            load_durations(path)
    finally:
        path.chmod(0o600)


def test_rebuild_command_sums_phases_and_replaces_older_measurements(tmp_path: Path) -> None:
    first = tmp_path / "first.log"
    second = tmp_path / "second.log"
    first.write_text(
        "job\tstep\t2026-11-05T00:00:00Z 10.00s setup tools/tests/test_example.py::test_a[x]\n"
        "job\tstep\t2026-11-05T00:00:00Z 20.00s call tools/tests/test_example.py::test_a[x]\n"
        "job\tstep\t2026-11-05T00:00:00Z 1.00s teardown tools/tests/test_example.py::test_a[x]\n"
        "2.00s call tools/tests/test_example.py::test_b\n"
    )
    second.write_text("40.00s call tools/tests/test_example.py::test_b\n")
    output = tmp_path / "durations.json"
    command = [
        sys.executable,
        str(Path(__file__).parents[1] / "rebuild_case_durations.py"),
        "--output",
        str(output),
    ]
    subprocess.run([*command, str(first), str(second)], check=True)
    assert json.loads(output.read_text()) == {
        "tools/tests/test_example.py::test_a[x]": 31,
        "tools/tests/test_example.py::test_b": 40,
    }
    second.write_text("no duration report\n")
    result = subprocess.run([*command, str(second)], capture_output=True, text=True)
    assert result.returncode != 0
