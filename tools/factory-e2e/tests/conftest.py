from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from case_shards import assign_case_shards, load_durations


@pytest.fixture(autouse=True)
def candidate_curie(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    binary = tmp_path / "bin" / "curie"
    binary.parent.mkdir()
    binary.write_text(
        '#!/bin/sh\nif [ "$1" = "--version" ]; then\n'
        "printf 'curie 0.0.0-test\\nadditional version detail\\n'\n"
        'else\nprintf "%s\\n" "$0"\nfi\n'
    )
    binary.chmod(0o755)
    monkeypatch.setenv("CURIE_BIN", str(binary))
    return binary


def _parse_case_shard(value: str) -> tuple[int, int]:
    index_text, separator, count_text = value.partition("/")
    if not separator or not index_text.isdigit() or not count_text.isdigit():
        raise pytest.UsageError(f"--case-shard must be INDEX/COUNT, got {value!r}")
    index, count = int(index_text), int(count_text)
    if not 1 <= index <= count:
        raise pytest.UsageError(f"--case-shard index must be within 1..{count}, got {value!r}")
    return index, count


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--case-shard",
        dest="case_shard",
        default=None,
        type=_parse_case_shard,
        metavar="INDEX/COUNT",
        help="Run one duration-balanced shard, preserving collection order within it.",
    )


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    shard = config.getoption("case_shard", default=None)
    if shard is None:
        return
    index, count = shard
    try:
        assignments = assign_case_shards([item.nodeid for item in items], count, load_durations())
    except (OSError, ValueError) as error:
        raise pytest.UsageError(f"Cannot load case shard durations: {error}") from error
    selected = set(assignments[index - 1])
    if not selected:
        raise pytest.UsageError(
            f"--case-shard {index}/{count} selects no case of {len(items)}; lower the count"
        )
    deselected = [item for item in items if item.nodeid not in selected]
    if deselected:
        config.hook.pytest_deselected(items=deselected)
    items[:] = [item for item in items if item.nodeid in selected]
