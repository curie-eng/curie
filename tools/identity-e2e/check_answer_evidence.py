"""Check one ``approver-ticket-answer`` evidence file (ADR 0271 items 3 and 4, slice 0271b).

Usage::

    python3 tools/identity-e2e/check_answer_evidence.py answer.json [--repo <checkout>]

It exits 0 only when the evidence names the ``approver-ticket-answer``
scenario, its overall ``passed`` is true, every required step id is present
and passed, and a candidate commit is recorded. Inside a git checkout it also
requires that ``git diff --name-only <candidate> HEAD`` touches nothing under
the runtime paths, so the harness that produced the evidence ran from the
build the evidence names. Outside a checkout that comparison is skipped and
the summary says so. Standard library only; on a mismatch it prints what it
observed.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from answer_authority_driver import SCENARIO, STEP_IDS

# What the evidence must have been produced against: a build whose runtime
# code equals the checkout's.
RUNTIME_PREFIXES = ("apps/", "packages/", "runner/", "cli/src/", "charts/")
COMMIT = re.compile(r"[0-9a-f]{7,40}")
FAILURE_LIMIT = 300
LISTED_PATHS = 20

# (exit code, stdout or the error text) for one ``git`` invocation.
GitRunner = Callable[[Sequence[str]], tuple[int, str]]


def run_git(args: Sequence[str]) -> tuple[int, str]:
    try:
        done = subprocess.run(  # noqa: S603
            ["git", *args], capture_output=True, text=True, check=False, timeout=60
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 127, type(exc).__name__
    if done.returncode != 0:
        return done.returncode, done.stderr.strip()[:FAILURE_LIMIT]
    return 0, done.stdout


def candidate_commit(evidence: Mapping[str, Any]) -> str | None:
    """The recorded candidate commit, when it is one."""

    value = evidence.get("candidate_commit")
    if isinstance(value, str) and COMMIT.fullmatch(value):
        return value
    return None


def step_results(evidence: Mapping[str, Any]) -> dict[str, list[Mapping[str, Any]]]:
    """Every recorded entry of each step id, in run order."""

    found: dict[str, list[Mapping[str, Any]]] = {}
    steps = evidence.get("steps")
    for entry in steps if isinstance(steps, list) else []:
        if isinstance(entry, Mapping) and isinstance(entry.get("id"), str):
            found.setdefault(entry["id"], []).append(entry)
    return found


def judge(evidence: Any) -> list[str]:
    """Every way the evidence falls short, one line each; empty when it passes."""

    if not isinstance(evidence, Mapping):
        return [f"the evidence is a JSON {type(evidence).__name__}, not an object"]
    problems = []
    scenario = evidence.get("scenario")
    if scenario != SCENARIO:
        problems.append(f"the evidence names scenario {scenario!r}, not {SCENARIO}")
    if evidence.get("passed") is not True:
        aborted = evidence.get("aborted")
        problems.append(
            f"overall passed is {evidence.get('passed')!r}"
            + (f"; aborted: {str(aborted)[:FAILURE_LIMIT]}" if aborted else "")
        )
    found = step_results(evidence)
    missing = [step for step in STEP_IDS if step not in found]
    if missing:
        problems.append(f"{len(missing)} required steps are missing: {', '.join(missing)}")
    for step in STEP_IDS:
        for entry in found.get(step, []):
            if entry.get("passed") is not True:
                failure = str(entry.get("failure") or "no failure recorded")[:FAILURE_LIMIT]
                problems.append(f"step {step} has passed={entry.get('passed')!r}: {failure}")
    if candidate_commit(evidence) is None:
        problems.append(
            f"no candidate commit is recorded (candidate_commit is "
            f"{evidence.get('candidate_commit')!r}; api_image is {evidence.get('api_image')!r})"
        )
    return problems


def runtime_paths(changed: Iterable[str]) -> list[str]:
    return sorted(path for path in changed if path.startswith(RUNTIME_PREFIXES))


def git_changed_paths(
    repo: Path, candidate: str, git: GitRunner = run_git
) -> list[str] | str | None:
    """Paths ``git diff --name-only <candidate> HEAD`` names in ``repo``'s checkout.

    None when ``repo`` is not inside a git checkout; the error text when the
    diff cannot be computed (for example, the candidate commit is not fetched).
    """

    code, top = git(["-C", str(repo), "rev-parse", "--show-toplevel"])
    if code != 0 or not top.strip():
        return None
    code, out = git(["-C", top.strip(), "diff", "--name-only", candidate, "HEAD"])
    if code != 0:
        return f"git diff --name-only {candidate} HEAD failed: {out}"
    return [line.strip() for line in out.splitlines() if line.strip()]


def checkout_problems(repo: Path, candidate: str | None, git: GitRunner) -> tuple[str, list[str]]:
    """The checkout comparison as one summary line, and its problems."""

    if candidate is None:
        return "checkout: not compared (no candidate commit)", []
    changed = git_changed_paths(repo, candidate, git)
    if changed is None:
        return f"checkout: {repo} is not a git checkout; comparison skipped", []
    if isinstance(changed, str):
        return "checkout: could not compare", [f"{changed}; fetch the candidate commit"]
    touched = runtime_paths(changed)
    if touched:
        shown = ", ".join(touched[:LISTED_PATHS])
        more = f" and {len(touched) - LISTED_PATHS} more" if len(touched) > LISTED_PATHS else ""
        return "checkout: differs from the candidate in runtime paths", [
            f"HEAD differs from candidate {candidate} under {', '.join(RUNTIME_PREFIXES)}: "
            f"{shown}{more}"
        ]
    return (
        f"checkout: HEAD matches the candidate under {', '.join(RUNTIME_PREFIXES)} "
        f"({len(changed)} other paths differ)",
        [],
    )


def main(argv: Sequence[str] | None = None, *, git: GitRunner = run_git) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("evidence", type=Path)
    parser.add_argument(
        "--repo",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="the checkout to compare with the candidate commit (default: this one)",
    )
    args = parser.parse_args(argv)
    try:
        evidence = json.loads(args.evidence.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"{args.evidence}: unreadable evidence ({type(exc).__name__}: {exc})")
        return 2
    problems = judge(evidence)
    data = evidence if isinstance(evidence, Mapping) else {}
    candidate = candidate_commit(data)
    line, more = checkout_problems(args.repo, candidate, git)
    problems += more
    found = step_results(data)
    passed = sum(
        1
        for step in STEP_IDS
        if found.get(step) and all(entry.get("passed") is True for entry in found[step])
    )
    print(f"{args.evidence}: scenario {data.get('scenario')!r}, passed {data.get('passed')!r}")
    print(f"steps: {passed} of {len(STEP_IDS)} required steps passed")
    print(f"candidate commit: {candidate or 'none recorded'}")
    print(line)
    if problems:
        print("MISMATCH")
        for index, problem in enumerate(problems, 1):
            print(f"{index}. {problem}")
        return 1
    print("OK: ADR 0271 items 3 and 4 evidence is complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
