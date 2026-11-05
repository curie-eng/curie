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
        ["unknown", "large"], ["small", "equal-b", "equal-a"]
    ]


@pytest.mark.parametrize("data", [{}, {"case": -1}, {"case": "bad"}])
def test_invalid_duration_file_is_refused(tmp_path: Path, data: object) -> None:
    path = tmp_path / "durations.json"
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        load_durations(path)


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
    command = [sys.executable, str(Path(__file__).parents[1] / "rebuild_case_durations.py"),
               "--output", str(output)]
    subprocess.run([*command, str(first), str(second)], check=True)
    assert json.loads(output.read_text()) == {
        "tools/tests/test_example.py::test_a[x]": 31,
        "tools/tests/test_example.py::test_b": 40,
    }
    second.write_text("no duration report\n")
    result = subprocess.run([*command, str(second)], capture_output=True, text=True)
    assert result.returncode != 0
