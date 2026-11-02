"""Execute the root scheduling plugin with isolated pytest and xdist processes."""

from pathlib import Path

import pytest

pytest_plugins = ["pytester"]

ROOT = Path(__file__).resolve().parents[3]


def _suite(pytester: pytest.Pytester, paths: list[str], *, independent: bool) -> None:
    pytester.makeconftest((ROOT / "conftest.py").read_text())
    pytester.makeini("[pytest]\n")
    for index, relative in enumerate(paths):
        path = pytester.path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "import pytest\nfrom pathlib import Path\n"
            + ("pytestmark = pytest.mark.xdist_independent\n" if independent else "")
            + "@pytest.mark.parametrize('case', range(8))\n"
            "def test_worker(case, request):\n"
            f"    record = Path({str(pytester.path / f'workers-{index}')!r})\n"
            "    record.mkdir(exist_ok=True)\n"
            "    (record / str(case)).write_text(request.config.workerinput['workerid'])\n"
        )


@pytest.mark.parametrize("independent", [False, True])
def test_independent_cases_use_multiple_workers_only_when_opted_in(
    pytester: pytest.Pytester, independent: bool
) -> None:
    _suite(pytester, ["test_isolated.py"], independent=independent)
    result = pytester.runpytest_subprocess("-q", "-n", "2", "--dist", "loadgroup")
    result.assert_outcomes(passed=8)
    workers = {path.read_text() for path in (pytester.path / "workers-0").iterdir()}
    assert len(workers) == (2 if independent else 1)


@pytest.mark.parametrize(
    "paths",
    [
        [
            "apps/api/tests/test_control_integration.py",
            "apps/worker/tests/kernel/test_consumer.py",
        ],
        [
            "apps/api/tests/test_guarded_nomination_api_integration.py",
            "apps/worker/tests/test_commissioning_record_refusal.py",
        ],
    ],
    ids=["shared-live-state", "protected-runner-port"],
)
def test_external_state_stays_serial_even_with_independent_marker(
    pytester: pytest.Pytester, paths: list[str]
) -> None:
    _suite(pytester, paths, independent=True)
    result = pytester.runpytest_subprocess("-q", "-n", "2", "--dist", "loadgroup")
    result.assert_outcomes(passed=16)
    workers = {
        path.read_text()
        for index in range(2)
        for path in (pytester.path / f"workers-{index}").iterdir()
    }
    assert len(workers) == 1


def test_independent_shards_partition_cases_and_keep_shared_groups_together(
    pytester: pytest.Pytester,
) -> None:
    paths = [
        "test_isolated.py",
        "apps/api/tests/test_control_integration.py",
        "apps/worker/tests/kernel/test_consumer.py",
    ]
    _suite(pytester, paths, independent=True)
    selected: list[set[str]] = []
    for index in range(1, 4):
        result = pytester.runpytest_subprocess("--collect-only", "-q", "--ci-shard", f"{index}/3")
        assert result.ret == 0
        selected.append({line for line in result.outlines if "::test_worker[" in line})
    assert sum(map(len, selected)) == len(set.union(*selected)) == 24
    isolated = [
        {node for node in shard if node.startswith("test_isolated.py::")} for shard in selected
    ]
    assert sum(bool(shard) for shard in isolated) > 1
    shared = [{node for node in shard if node.startswith("apps/")} for shard in selected]
    assert sorted(map(len, shared)) == [0, 0, 16]
