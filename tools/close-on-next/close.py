"""Label issues whose fix merged to next; close them when next reaches main.

GitHub closing keywords fire only on the default branch. This helper is the
non-default-branch half of that contract: parse merged pull request bodies the
way GitHub would, apply `fixed-on-next`, and close those issues when `next`
merges to `main`.

Choice: the label path, not close-on-merge-to-next. Closing on `next` would
under-count remaining work on the patch train. See the workflow header.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

LABEL = "fixed-on-next"
LABEL_COLOR = "1D76DB"
LABEL_DESCRIPTION = "Fix merged to next; closes when next merges to main"
COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
MERGE_FROM_NEXT = re.compile(
    r"(?i)"
    r"(?:^Merge (?:remote-tracking )?branch '(?:origin/)?next'(?: into \S+)?$)"
    r"|(?:^Merge pull request #\d+ from \S+/next(?:\s|$))"
)
ZERO_SHA = "0" * 40


@dataclass(frozen=True)
class PullRequest:
    """A merged pull request whose body may name issues to close."""

    number: int
    body: str
    url: str
    merged: bool
    base: str
    head: str


@dataclass(frozen=True)
class Issue:
    """Enough of a GitHub issue to decide label vs skip vs close."""

    number: int
    state: str
    labels: tuple[str, ...]


@dataclass(frozen=True)
class Action:
    """One GitHub mutation, or the plan of one in dry-run."""

    kind: str
    issue: int | None = None
    body: str | None = None
    reason: str = ""


def parse_closing_issues(body: str, repository: str) -> tuple[int, ...]:
    """Return same-repo issue numbers GitHub would close from this body.

    GitHub requires a closing keyword immediately before each reference, so
    `Closes #12, #13` yields only 12. HTML comments are stripped first. A `#`
    glued to a word character or a slash is a cross-repo reference and is
    ignored, matching GitHub's same-repository rule.
    """
    owner, _, name = repository.partition("/")
    if not owner or not name:
        raise ValueError(f"invalid repository slug: {repository}")
    repo_url = re.escape(f"https://github.com/{owner}/{name}/issues/")
    pattern = re.compile(
        r"\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\b\s*:?\s*"
        rf"(?:{repo_url}|(?<![\w/])#)"
        r"(?P<number>\d+)",
        re.IGNORECASE,
    )
    uncommented = COMMENT.sub("", body)
    numbers: list[int] = []
    for match in pattern.finditer(uncommented):
        number = int(match.group("number"))
        if number == 0 or number in numbers:
            continue
        numbers.append(number)
    return tuple(numbers)


def label_comment(pr: PullRequest) -> str:
    return (
        f"Fixed on `next` by {pr.url} (#{pr.number}). "
        "This issue stays open until `next` merges into `main`; "
        f"the `{LABEL}` label tracks that."
    )


def close_comment() -> str:
    return (
        f"Closed because `next` merged into `main`. "
        f"Tracked as `{LABEL}` since the fix landed on the feature train."
    )


def plan_next_actions(
    prs: Sequence[PullRequest],
    open_issues: Mapping[int, Issue],
    *,
    repository: str,
) -> list[Action]:
    """Label open issues named by merged-to-next PR bodies; skip the rest."""
    actions: list[Action] = []
    seen: set[int] = set()
    for pr in prs:
        if not pr.merged or pr.base != "next":
            continue
        for number in parse_closing_issues(pr.body, repository):
            if number in seen:
                continue
            seen.add(number)
            issue = open_issues.get(number)
            if issue is None:
                continue
            if LABEL in issue.labels:
                continue
            actions.append(
                Action(kind="add_label", issue=number, reason=f"PR #{pr.number}")
            )
            actions.append(
                Action(
                    kind="comment",
                    issue=number,
                    body=label_comment(pr),
                    reason=f"PR #{pr.number}",
                )
            )
    if not actions:
        return []
    return [Action(kind="ensure_label", reason=f"create `{LABEL}` if missing")] + actions


def plan_main_actions(
    *, should_close: bool, labeled_open: Sequence[Issue]
) -> list[Action]:
    """Close open `fixed-on-next` issues only when next just merged to main."""
    if not should_close:
        return []
    actions: list[Action] = []
    for issue in labeled_open:
        actions.append(
            Action(kind="comment", issue=issue.number, body=close_comment(), reason=LABEL)
        )
        actions.append(Action(kind="close", issue=issue.number, reason=LABEL))
    return actions


def should_close_labeled_issues(
    *,
    ref: str,
    event_name: str,
    head_message: str,
    associated_is_next_merge: bool,
    next_is_ancestor_of_after: bool | None,
    next_is_ancestor_of_before: bool | None,
) -> bool:
    """True when this event is a `next` into `main` merge (or dispatch recovery)."""
    short = ref.rsplit("/", 1)[-1]
    if short != "main":
        return False
    if associated_is_next_merge:
        return True
    if MERGE_FROM_NEXT.search(head_message.strip()):
        return True
    if next_is_ancestor_of_after is True:
        if event_name == "workflow_dispatch":
            return True
        if next_is_ancestor_of_before is False:
            return True
    return False


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--event", type=Path)
    parser.add_argument("--repo")
    parser.add_argument("--ref")
    return parser


def _load_event(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"could not read event: {error}") from error
    if not isinstance(payload, dict):
        raise ValueError("event must be an object")
    return payload


def _repo(event: dict[str, Any] | None, explicit: str | None) -> str:
    if explicit:
        return explicit
    if event is not None:
        repository = event.get("repository")
        if isinstance(repository, dict):
            full_name = repository.get("full_name")
            if isinstance(full_name, str) and full_name:
                return full_name
    env = os.environ.get("GITHUB_REPOSITORY")
    if env:
        return env
    raise ValueError("no repository slug is available")


def _ref(event: dict[str, Any] | None, explicit: str | None) -> str:
    if explicit:
        return explicit
    if event is not None:
        value = event.get("ref")
        if isinstance(value, str) and value:
            return value
    env = os.environ.get("GITHUB_REF")
    if env:
        return env
    raise ValueError("no git ref is available")


def _event_name(event: dict[str, Any] | None) -> str:
    env = os.environ.get("GITHUB_EVENT_NAME")
    if env:
        return env
    if event is not None and "inputs" in event:
        return "workflow_dispatch"
    return "push"


def _head_message(event: dict[str, Any] | None) -> str:
    if event is None:
        return ""
    commit = event.get("head_commit")
    if isinstance(commit, dict):
        message = commit.get("message")
        if isinstance(message, str):
            return message
    return ""


def _sha(event: dict[str, Any] | None, field: str, fallback: str | None) -> str | None:
    if event is not None:
        value = event.get(field)
        if isinstance(value, str) and value and value != ZERO_SHA:
            return value
    if fallback:
        return fallback
    return None


def _gh(*args: str) -> str:
    gh = shutil.which("gh")
    if gh is None:
        raise ValueError("gh is not on PATH")
    env = os.environ.copy()
    # gh colorizes JSON when stdout is captured unless color is forced off,
    # and a pager can swallow the payload. Both make json.loads fail closed.
    env["NO_COLOR"] = "1"
    env["CLICOLOR"] = "0"
    env["GH_PAGER"] = "cat"
    env.pop("FORCE_COLOR", None)
    env.pop("CLICOLOR_FORCE", None)
    completed = subprocess.run(
        [gh, *args], capture_output=True, check=False, text=True, env=env
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise ValueError(f"gh {' '.join(args)} failed: {detail}")
    return completed.stdout


def _json_list(text: str, what: str) -> list[Any]:
    try:
        payload = json.loads(text) if text.strip() else []
    except json.JSONDecodeError as error:
        raise ValueError(f"could not parse {what}: {error}") from error
    if not isinstance(payload, list):
        raise ValueError(f"{what} must be a JSON array")
    return payload


def _labels_of(raw: Any) -> tuple[str, ...]:
    if not isinstance(raw, list):
        return ()
    names: list[str] = []
    for item in raw:
        if isinstance(item, str):
            names.append(item)
        elif isinstance(item, dict):
            name = item.get("name")
            if isinstance(name, str):
                names.append(name)
    return tuple(names)


def _issue_from(raw: Any) -> Issue | None:
    if not isinstance(raw, dict):
        return None
    number = raw.get("number")
    if not isinstance(number, int):
        return None
    state = raw.get("state")
    state_text = state.lower() if isinstance(state, str) else "open"
    return Issue(number=number, state=state_text, labels=_labels_of(raw.get("labels")))


def _pr_from(raw: Any) -> PullRequest | None:
    if not isinstance(raw, dict):
        return None
    number = raw.get("number")
    if not isinstance(number, int):
        return None
    body = raw.get("body")
    body_text = body if isinstance(body, str) else ""
    url = raw.get("url") or raw.get("html_url") or ""
    url_text = url if isinstance(url, str) else ""
    base = raw.get("baseRefName")
    head = raw.get("headRefName")
    if isinstance(raw.get("base"), dict):
        base = raw["base"].get("ref", base)
    if isinstance(raw.get("head"), dict):
        head = raw["head"].get("ref", head)
    merged = bool(raw.get("mergedAt") or raw.get("merged_at") or raw.get("merged"))
    return PullRequest(
        number=number,
        body=body_text,
        url=url_text,
        merged=merged,
        base=base if isinstance(base, str) else "",
        head=head if isinstance(head, str) else "",
    )


def _list_merged_next_prs(repo: str) -> list[PullRequest]:
    text = _gh(
        "pr",
        "list",
        "--repo",
        repo,
        "--base",
        "next",
        "--state",
        "merged",
        "--limit",
        "1000",
        "--json",
        "number,url,body,title,mergedAt,headRefName,baseRefName",
    )
    prs: list[PullRequest] = []
    for item in _json_list(text, "merged pull requests"):
        parsed = _pr_from(item)
        if parsed is not None:
            prs.append(parsed)
    return prs


def _list_open_issues(repo: str) -> dict[int, Issue]:
    text = _gh(
        "issue",
        "list",
        "--repo",
        repo,
        "--state",
        "open",
        "--limit",
        "1000",
        "--json",
        "number,state,labels",
    )
    issues: dict[int, Issue] = {}
    for item in _json_list(text, "open issues"):
        parsed = _issue_from(item)
        if parsed is not None:
            issues[parsed.number] = parsed
    return issues


def _list_labeled_open(repo: str) -> list[Issue]:
    text = _gh(
        "issue",
        "list",
        "--repo",
        repo,
        "--label",
        LABEL,
        "--state",
        "open",
        "--limit",
        "1000",
        "--json",
        "number,state,labels,title,url",
    )
    issues: list[Issue] = []
    for item in _json_list(text, "labeled issues"):
        parsed = _issue_from(item)
        if parsed is not None:
            issues.append(parsed)
    return issues


def _associated_is_next_merge(repo: str, sha: str | None) -> bool:
    if not sha:
        return False
    try:
        text = _gh("api", f"repos/{repo}/commits/{sha}/pulls")
    except ValueError:
        return False
    for item in _json_list(text, "associated pull requests"):
        parsed = _pr_from(item)
        if (
            parsed is not None
            and parsed.base == "main"
            and parsed.head == "next"
            and parsed.merged
        ):
            return True
    return False


def _is_ancestor(ancestor: str, descendant: str) -> bool | None:
    completed = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        capture_output=True,
        check=False,
    )
    if completed.returncode == 0:
        return True
    if completed.returncode == 1:
        return False
    return None


def _ensure_origin_next() -> None:
    subprocess.run(
        ["git", "fetch", "--no-tags", "origin", "next:refs/remotes/origin/next"],
        capture_output=True,
        check=False,
    )


def _print_plan(actions: Sequence[Action]) -> None:
    payload = [
        {
            "kind": action.kind,
            "issue": action.issue,
            "reason": action.reason,
            "body": action.body,
        }
        for action in actions
    ]
    print(json.dumps({"actions": payload}, indent=2))
    if not actions:
        print("no actions")


def _apply(repo: str, actions: Sequence[Action]) -> None:
    for action in actions:
        if action.kind == "ensure_label":
            _gh(
                "label",
                "create",
                LABEL,
                "--repo",
                repo,
                "--color",
                LABEL_COLOR,
                "--description",
                LABEL_DESCRIPTION,
                "--force",
            )
        elif action.kind == "add_label":
            _gh(
                "issue",
                "edit",
                str(action.issue),
                "--repo",
                repo,
                "--add-label",
                LABEL,
            )
        elif action.kind == "comment":
            _gh(
                "issue",
                "comment",
                str(action.issue),
                "--repo",
                repo,
                "--body",
                action.body or "",
            )
        elif action.kind == "close":
            _gh(
                "issue",
                "close",
                str(action.issue),
                "--repo",
                repo,
                "--reason",
                "completed",
            )
        else:
            raise ValueError(f"unknown action {action.kind}")


def _self_test() -> None:
    repo = "curie-eng/curie"
    assert parse_closing_issues("Fixes #2201\n", repo) == (2201,)
    assert parse_closing_issues("Closes #12, #13\n", repo) == (12,)
    assert parse_closing_issues("Closes #12, closes #13\n", repo) == (12, 13)
    assert parse_closing_issues("<!-- Closes #12 -->\n", repo) == ()
    assert parse_closing_issues("This mentions #12 without a keyword.\n", repo) == ()
    pr = PullRequest(
        number=2217,
        body="Fixes #2201\n",
        url="https://github.com/curie-eng/curie/pull/2217",
        merged=True,
        base="next",
        head="task/example",
    )
    labeled = plan_next_actions(
        [pr],
        {2201: Issue(number=2201, state="open", labels=())},
        repository=repo,
    )
    assert any(action.kind == "add_label" and action.issue == 2201 for action in labeled)
    skipped = plan_next_actions(
        [pr],
        {2201: Issue(number=2201, state="open", labels=(LABEL,))},
        repository=repo,
    )
    assert skipped == []
    closed = plan_main_actions(
        should_close=True,
        labeled_open=[Issue(number=12, state="open", labels=(LABEL,))],
    )
    assert [action.kind for action in closed] == ["comment", "close"]
    assert (
        plan_main_actions(
            should_close=False,
            labeled_open=[Issue(number=12, state="open", labels=(LABEL,))],
        )
        == []
    )
    assert should_close_labeled_issues(
        ref="refs/heads/main",
        event_name="push",
        head_message="Merge pull request #99 from example/next",
        associated_is_next_merge=False,
        next_is_ancestor_of_after=None,
        next_is_ancestor_of_before=None,
    )
    assert not should_close_labeled_issues(
        ref="refs/heads/main",
        event_name="push",
        head_message="Merge pull request #50 from example/task/hotfix",
        associated_is_next_merge=False,
        next_is_ancestor_of_after=None,
        next_is_ancestor_of_before=None,
    )
    print("close-on-next self-test passed: parser, planner, and merge detection")


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    if arguments.self_test:
        try:
            _self_test()
        except AssertionError as error:
            print(f"self-test failed: {error}", file=sys.stderr)
            return 1
        return 0

    try:
        event = _load_event(arguments.event) if arguments.event is not None else None
        repo = _repo(event, arguments.repo)
        ref = _ref(event, arguments.ref)
        event_name = _event_name(event)
        short = ref.rsplit("/", 1)[-1]
        if short == "next":
            prs = _list_merged_next_prs(repo)
            open_issues = _list_open_issues(repo)
            actions = plan_next_actions(prs, open_issues, repository=repo)
        elif short == "main":
            after = _sha(event, "after", os.environ.get("GITHUB_SHA"))
            before = _sha(event, "before", None)
            associated = _associated_is_next_merge(repo, after)
            _ensure_origin_next()
            after_has = _is_ancestor("origin/next", after) if after else None
            before_has = _is_ancestor("origin/next", before) if before else None
            should_close = should_close_labeled_issues(
                ref=ref,
                event_name=event_name,
                head_message=_head_message(event),
                associated_is_next_merge=associated,
                next_is_ancestor_of_after=after_has,
                next_is_ancestor_of_before=before_has,
            )
            labeled = _list_labeled_open(repo) if should_close else []
            actions = plan_main_actions(should_close=should_close, labeled_open=labeled)
        else:
            print(f"skip: ref {ref} is not next or main")
            return 0
        _print_plan(actions)
        if arguments.dry_run:
            print("dry-run: no GitHub mutations")
            return 0
        _apply(repo, actions)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
