"""Decide whether helm-ci runs the chart shards, and whether they passed."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from collections.abc import Iterable, Mapping, Sequence

# This is the former helm-ci pull_request paths filter. The chart scripts
# read or execute these trees, so a pull request that changes one must still
# run the shards. Push events ignore the list and always run the shards,
# because release authorization requires the chart check to execute on every
# push to main and next.
CHART_PATHS: tuple[str, ...] = (
    "charts/curie/**",
    "examples/sre-bot/**",
    ".github/workflows/helm-ci.yaml",
    ".github/workflows/ci.yaml",
    ".github/workflows/release.yaml",
    "cli/**",
    "apps/api/**",
    "apps/worker/**",
    "apps/dispatcher/**",
    "packages/**",
    "scripts/**",
    "uv.lock",
    "pyproject.toml",
    "compose.yaml",
    "compose.dev.yaml",
    "cli/src/ops/upgrade.rs",
    "cli/tests/data/upgrade-driver.py",
    "packages/aci-protocol/src/aci_protocol/slack_identities.py",
    "packages/aci-protocol/src/aci_protocol/turn.py",
    "apps/worker/src/curie_worker/sandbox/types.py",
    "compose/**",
)

_SHARDS = frozenset({"lint", "render", "retained"})


def _compile(pattern: str) -> re.Pattern[str]:
    """Translate a GitHub filter pattern. `*` does not match `/`; `**` does."""
    parts: list[str] = []
    index = 0
    while index < len(pattern):
        if pattern.startswith("**", index):
            parts.append(".*")
            index += 2
        elif pattern[index] == "*":
            parts.append("[^/]*")
            index += 1
        else:
            parts.append(re.escape(pattern[index]))
            index += 1
    return re.compile("".join(parts))


_COMPILED = tuple(_compile(pattern) for pattern in CHART_PATHS)


def _normalize(path: str) -> str:
    if path.startswith("./"):
        return path[2:]
    return path


def chart_relevant(paths: Iterable[str]) -> bool:
    """True when any changed path matches the historical chart filter."""
    for path in paths:
        normalized = _normalize(path)
        if any(pattern.fullmatch(normalized) for pattern in _COMPILED):
            return True
    return False


def select_chart(event: str, changed: Sequence[str]) -> bool:
    """Whether this event should run the chart shards."""
    if event == "push":
        return True
    if event == "pull_request":
        return chart_relevant(changed)
    raise SystemExit(1)


def aggregate_ok(changes_result: str, chart: str, shards: Mapping[str, str]) -> bool:
    """True only for a successful selector plus a consistent shard triple."""
    if changes_result != "success" or chart not in {"true", "false"}:
        return False
    if set(shards) != _SHARDS:
        return False
    expected = "success" if chart == "true" else "skipped"
    return all(value == expected for value in shards.values())


def _write_chart(value: str) -> None:
    line = f"chart={value}\n"
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            handle.write(line)
        return
    sys.stdout.write(line)


def _changed_pull_request_paths() -> list[str]:
    base = os.environ.get("BASE_REF", "")
    if not base:
        raise SystemExit(1)
    fetch = subprocess.run(
        ["git", "fetch", "--quiet", "origin", base],
        check=False,
    )
    if fetch.returncode != 0:
        raise SystemExit(1)
    # -z keeps names raw. Without it, git quotes non-ASCII and special
    # characters, and a chart path such as templates/café.yaml matches nothing.
    diff = subprocess.run(
        ["git", "diff", "-z", "--name-only", "--no-renames", f"origin/{base}...HEAD"],
        check=False,
        capture_output=True,
    )
    if diff.returncode != 0:
        raise SystemExit(1)
    return [
        part.decode("utf-8", "surrogateescape")
        for part in diff.stdout.split(b"\0")
        if part
    ]


def cmd_select() -> None:
    event = os.environ.get("EVENT_NAME", "")
    if event == "push":
        _write_chart("true")
        return
    if event != "pull_request":
        raise SystemExit(1)
    changed = _changed_pull_request_paths()
    _write_chart("true" if select_chart(event, changed) else "false")


def _flipped(shards: Mapping[str, str], name: str) -> dict[str, str]:
    mutated = dict(shards)
    mutated[name] = "failure" if shards[name] == "success" else "success"
    return mutated


def cmd_aggregate() -> None:
    changes_result = os.environ.get("CHANGES_RESULT", "")
    chart = os.environ.get("CHART", "")
    shards = {
        "lint": os.environ.get("SHARD_LINT", ""),
        "render": os.environ.get("SHARD_RENDER", ""),
        "retained": os.environ.get("SHARD_RETAINED", ""),
    }
    if not aggregate_ok(changes_result, chart, shards):
        print("chart shard outcomes are not an allowed pair", file=sys.stderr)
        raise SystemExit(1)
    for name in shards:
        if aggregate_ok(changes_result, chart, _flipped(shards, name)):
            print(f"negative control still passed after flipping {name}", file=sys.stderr)
            raise SystemExit(1)
    print("negative control passed")


def main(argv: Sequence[str]) -> None:
    if len(argv) != 2 or argv[1] not in {"select", "aggregate"}:
        print("usage: decide.py select|aggregate", file=sys.stderr)
        raise SystemExit(2)
    if argv[1] == "select":
        cmd_select()
        return
    cmd_aggregate()


if __name__ == "__main__":
    main(sys.argv)
