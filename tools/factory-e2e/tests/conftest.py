from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


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


# Contiguous collection-order shards for the serial kind regression modules.
# Every case there provisions its own kind cluster, so a module can run as
# parallel CI jobs. A shard is a contiguous block of ceil(total / count) cases,
# so each case keeps the cases that precede it in the same process and the
# shards partition the collection exactly. The split depends only on the
# collected order, which every runner computes alike.


def _parse_case_shard(value: str) -> tuple[int, int]:
    index_text, separator, count_text = value.partition("/")
    if not separator or not index_text.isdigit() or not count_text.isdigit():
        raise pytest.UsageError(f"--case-shard must be INDEX/COUNT, got {value!r}")
    index, count = int(index_text), int(count_text)
    if not 1 <= index <= count:
        raise pytest.UsageError(f"--case-shard index must be within 1..{count}, got {value!r}")
    return index, count


def case_shard_bounds(total: int, index: int, count: int) -> tuple[int, int]:
    """The half-open collection range of shard ``index`` of ``count``."""

    size = math.ceil(total / count)
    start, end = (index - 1) * size, min(index * size, total)
    if start >= end:
        raise pytest.UsageError(
            f"--case-shard {index}/{count} selects no case of {total}; lower the count"
        )
    return start, end


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--case-shard",
        dest="case_shard",
        default=None,
        type=_parse_case_shard,
        metavar="INDEX/COUNT",
        help="Run one contiguous collection-order block of the selected cases.",
    )


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    shard = config.getoption("case_shard", default=None)
    if shard is None:
        return
    start, end = case_shard_bounds(len(items), *shard)
    deselected = items[:start] + items[end:]
    if deselected:
        config.hook.pytest_deselected(items=deselected)
    items[:] = items[start:end]
