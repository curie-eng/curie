"""Where a factory execution request answers, read from its objective's first line.

A revision asked for from pull request review feedback (#2798) carries the
canonical feedback URL as the first objective line. Only that line is read,
and only a URL on the WorkItem's own repository under the configured clone
base counts. Anything else answers on the issue, as before.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

_FRAGMENTS = {
    "discussion_r": "thread",
    "issuecomment-": "pr",
    "pullrequestreview-": "pr",
}
_MAX_ID = 2**63 - 1
_MAX_PR = 2**31 - 1


@dataclass(frozen=True)
class ReplyTarget:
    kind: Literal["thread", "pr", "issue"]
    pr_number: int | None = None
    comment_id: int | None = None
    url: str | None = None


ISSUE_TARGET = ReplyTarget("issue")


def feedback_url(clone_base: str, repo_full_name: str, pr_number: int, fragment: str) -> str:
    return f"{clone_base.rstrip('/')}/{repo_full_name}/pull/{pr_number}#{fragment}"


def parse_reply_target(
    objective: str | None, *, repo_full_name: str, clone_base: str
) -> ReplyTarget:
    if not isinstance(objective, str) or not objective:
        return ISSUE_TARGET
    first = objective.split("\n", 1)[0]
    pattern = (
        rf"{re.escape(clone_base.rstrip('/'))}/{re.escape(repo_full_name)}"
        r"/pull/([1-9][0-9]{0,9})#(discussion_r|issuecomment-|pullrequestreview-)([1-9][0-9]{0,18})"
    )
    match = re.fullmatch(pattern, first)
    if match is None:
        return ISSUE_TARGET
    pr_number, comment_id = int(match.group(1)), int(match.group(3))
    if pr_number > _MAX_PR or comment_id > _MAX_ID:
        return ISSUE_TARGET
    kind: Literal["thread", "pr"] = "thread" if _FRAGMENTS[match.group(2)] == "thread" else "pr"
    return ReplyTarget(kind, pr_number, comment_id, first)
