"""Comment when a pull request Closes a P0, release-blocker, or sre-bot-e2e-demo issue.

Advisory only: never fails a pull request because it Closes one of those
labels. The implement skill owns the spec-vs-impl AC-vs-diff pass; this helper
only reminds. Do not raise GitHub required reviews.

Closing-keyword parsing is a sibling of ``tools/fix-pin-ci/check.py``. That
helper asks for a Fix pin when a ``bug`` closes; this one comments when a
``P0``, ``release-blocker``, or ``sre-bot-e2e-demo`` issue closes. The regex is
duplicated because changing Fix pin parsing is out of scope for #2306.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
# Sibling of tools/fix-pin-ci/check.py::CLOSING. GitHub closes an issue in the
# same repository only for a bare `#N` reference that follows a closing
# keyword. A `#` glued to a word character or a slash is a cross repository or
# URL reference and closes nothing here. The single optional colon covers
# GitHub's documented `Closes: #N` form.
CLOSING = re.compile(
    r"\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\b\s*:?"
    r"(?P<references>(?:\s*(?:,|and)?\s*(?<![\w/])#\d+)+)",
    re.IGNORECASE,
)
REFERENCE = re.compile(r"(?<![\w/])#(?P<number>\d+)")
TRIGGER_LABELS = frozenset({"P0", "release-blocker", "sre-bot-e2e-demo"})
COMMENT_MARKER = "<!-- curie-p0-closes-spec-vs-impl -->"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event", type=Path, required=True)
    parser.add_argument("--comment-file", type=Path, required=True)
    return parser


def _event(event_path: Path) -> dict[str, object]:
    try:
        event = json.loads(event_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"could not read pull request event: {error}") from error

    if not isinstance(event, dict):
        raise ValueError("pull request event must be an object")
    return event


def _body(event: dict[str, object]) -> str:
    pull_request = event.get("pull_request")
    if not isinstance(pull_request, dict):
        return ""

    body = pull_request.get("body")
    if body is None:
        return ""
    if not isinstance(body, str):
        raise ValueError("pull request body must be a string or null")
    return body


def _repository(event: dict[str, object]) -> str | None:
    repository = event.get("repository")
    if isinstance(repository, dict):
        full_name = repository.get("full_name")
        if isinstance(full_name, str) and full_name:
            return full_name
    return os.environ.get("GITHUB_REPOSITORY") or None


def _closed_issues(body: str) -> list[int]:
    uncommented = COMMENT.sub("", body)
    numbers: list[int] = []
    for closure in CLOSING.finditer(uncommented):
        for reference in REFERENCE.finditer(closure.group("references")):
            number = int(reference.group("number"))
            if number not in numbers:
                numbers.append(number)
    return numbers


def _labels(repository: str | None, issue: int) -> list[str]:
    gh = shutil.which("gh")
    if gh is None:
        raise ValueError(f"gh is not on PATH, so the labels of issue #{issue} cannot be read")
    if repository is None:
        raise ValueError(
            f"no repository slug is available, so the labels of issue #{issue} cannot be read"
        )

    completed = subprocess.run(
        [gh, "api", f"repos/{repository}/issues/{issue}", "--jq", "[.labels[].name]"],
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise ValueError(f"could not read the labels of issue #{issue}: {detail}")

    try:
        labels = json.loads(completed.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"could not parse the labels of issue #{issue}: {error}") from error

    if not isinstance(labels, list) or not all(isinstance(label, str) for label in labels):
        raise ValueError(f"could not parse the labels of issue #{issue}: not a list of names")
    return labels


def _trigger_issues(event: dict[str, object], body: str) -> list[int]:
    issues = _closed_issues(body)
    if not issues:
        return []
    repository = _repository(event)
    matched: list[int] = []
    for issue in issues:
        labels = _labels(repository, issue)
        if TRIGGER_LABELS.intersection(labels):
            matched.append(issue)
    return matched


def _comment_body(issues: Sequence[int]) -> str:
    listed = ", ".join(f"#{issue}" for issue in issues)
    return (
        f"{COMMENT_MARKER}\n"
        "This pull request uses a GitHub closing keyword on issue(s) labeled "
        f"`P0`, `release-blocker`, or `sre-bot-e2e-demo`: {listed}.\n"
        "\n"
        "Confirm each issue acceptance criterion is visible in the diff, not "
        "only in the e2e table or the Fix pin selector. A string or "
        "refusal-text test cannot be the sole pin for a routing, catalog, or "
        "live-trace AC. If the diff does not implement an AC, switch the "
        "keyword to `Ref` and leave the issue open.\n"
        "\n"
        "Documented insufficient pin: #2209 closed #2202 with a message-only "
        "Fix pin; #2248 reopened the routing AC.\n"
    )


def _write_comment_file(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")


def _skip(comment_file: Path, reason: str) -> int:
    _write_comment_file(comment_file, "")
    print(f"SKIPPED: {reason}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)

    try:
        event = _event(arguments.event)
        body = _body(event)
    except ValueError as error:
        print(f"P0 Closes reminder error: {error}", file=sys.stderr)
        return 1

    issues = _closed_issues(body)
    if not issues:
        return _skip(arguments.comment_file, "no P0, release-blocker, or sre-bot-e2e-demo Closes")

    try:
        matched = _trigger_issues(event, body)
    except ValueError as error:
        return _skip(arguments.comment_file, f"could not read issue labels ({error})")

    if not matched:
        return _skip(arguments.comment_file, "no P0, release-blocker, or sre-bot-e2e-demo Closes")

    listed = ", ".join(f"#{issue}" for issue in matched)
    _write_comment_file(arguments.comment_file, _comment_body(matched))
    print(f"COMMENT: this Closes a P0, release-blocker, or sre-bot-e2e-demo issue ({listed})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
