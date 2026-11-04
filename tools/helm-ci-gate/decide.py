"""Decide whether helm-ci runs the chart shards, and whether they passed."""

from __future__ import annotations

import json
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
    if event in {"pull_request", "merge_group"}:
        return chart_relevant(changed)
    raise SystemExit(1)


_SHARDS = frozenset({"lint", "render", "retained"})
# The kind jobs are deep tiers. An unlabeled pull request defers them to the
# merge queue; every other event must run them when the chart is selected.
_KIND_JOBS = {
    "reserved_env": "Reject legacy extraEnv before upgrade mutation",
    "thread_isolation": "Automatic thread isolation (kind)",
}
_RESULT_KEYS = _SHARDS | frozenset(_KIND_JOBS)
_BOOLEANS = frozenset({"true", "false"})


def aggregate_ok(
    changes_result: str, event: str, chart: str, deep: str, results: Mapping[str, str],
) -> bool:
    """True only for a successful selector plus the selected result for every job."""
    if changes_result != "success" or chart not in _BOOLEANS or deep not in _BOOLEANS:
        return False
    if event != "pull_request" and deep != "true":
        return False
    if set(results) != _RESULT_KEYS:
        return False
    for name, value in results.items():
        selected = chart == "true" and (name in _SHARDS or deep == "true")
        if value != ("success" if selected else "skipped"):
            return False
    return True


def deferred(chart: str, deep: str) -> list[str]:
    """Display names of the kind jobs a selected chart defers to the merge queue."""
    if chart == "true" and deep == "false":
        return list(_KIND_JOBS.values())
    return []


def _write_outputs(chart: bool, deep: bool) -> None:
    text = f"chart={str(chart).lower()}\ndeep={str(deep).lower()}\n"
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            handle.write(text)
        return
    sys.stdout.write(text)


def _changed_merge_group_paths() -> list[str]:
    with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as handle:
        group = json.load(handle)["merge_group"]
    base, head = group["base_sha"], group["head_sha"]
    if not all(isinstance(sha, str) and re.fullmatch(r"[0-9a-f]{40}", sha)
               for sha in (base, head)):
        raise SystemExit(1)
    diff = subprocess.run(
        ["git", "diff", "-z", "--name-only", "--no-renames", f"{base}..{head}"],
        check=True, capture_output=True,
    )
    return [part.decode("utf-8", "surrogateescape") for part in diff.stdout.split(b"\0") if part]


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
        _write_outputs(True, True)
        return
    if event == "merge_group":
        changed = _changed_merge_group_paths()
        deep = True
    elif event == "pull_request":
        changed = _changed_pull_request_paths()
        deep = os.environ.get("DEEP_LABEL", "") == "true"
    else:
        raise SystemExit(1)
    _write_outputs(select_chart(event, changed), deep)


def _flipped(results: Mapping[str, str], name: str) -> dict[str, str]:
    mutated = dict(results)
    mutated[name] = "failure" if results[name] == "success" else "success"
    return mutated


def cmd_aggregate() -> None:
    changes_result = os.environ.get("CHANGES_RESULT", "")
    event = os.environ.get("EVENT_NAME", "")
    chart = os.environ.get("CHART", "")
    deep = os.environ.get("DEEP", "")
    results = {
        "lint": os.environ.get("SHARD_LINT", ""),
        "render": os.environ.get("SHARD_RENDER", ""),
        "retained": os.environ.get("SHARD_RETAINED", ""),
        "reserved_env": os.environ.get("KIND_RESERVED_ENV", ""),
        "thread_isolation": os.environ.get("KIND_THREAD_ISOLATION", ""),
    }
    if not aggregate_ok(changes_result, event, chart, deep, results):
        print("chart job outcomes are not the selected outcomes", file=sys.stderr)
        raise SystemExit(1)
    for name in results:
        if aggregate_ok(changes_result, event, chart, deep, _flipped(results, name)):
            print(f"negative control still passed after flipping {name}", file=sys.stderr)
            raise SystemExit(1)
    print("negative control passed")
    lines = [f"Deferred to the merge queue: {name}" for name in deferred(chart, deep)]
    for line in lines:
        print(line)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary and lines:
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write("".join(f"{line}\n" for line in lines))


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
