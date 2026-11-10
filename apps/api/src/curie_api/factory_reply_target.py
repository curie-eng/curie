"""Where a factory execution request answers (ADR 0197 decision fact 4).

Each request stores a typed reply target: the tracker issue, a pull request
conversation, or one review thread. Admission writes it from the feedback it
verified; readers take the stored columns and never parse the objective. A
revision's objective still opens with the feedback URL, for the agent only.
"""

from __future__ import annotations

import re

from .forges.types import PullRequestRef, ReplyTarget
from .models import ExecutionRequest, WorkItem


def feedback_url(clone_base: str, repo_full_name: str, pr_number: int, fragment: str) -> str:
    return f"{clone_base.rstrip('/')}/{repo_full_name}/pull/{pr_number}#{fragment}"


def reply_columns(target: ReplyTarget | None, url: str | None) -> dict[str, str | None]:
    """The execution request columns for ``target``; None is the tracker issue.

    ``url`` is the feedback the reply answers, kept for the "In response to"
    line. An issue reply carries none.
    """

    if target is None or target.kind == "issue":
        return {
            "reply_target_kind": "issue",
            "reply_target_pr_number": None,
            "reply_target_comment_id": None,
            "reply_target_url": None,
        }
    assert target.pull_request is not None
    return {
        "reply_target_kind": target.kind,
        "reply_target_pr_number": target.pull_request.number,
        "reply_target_comment_id": target.thread_id,
        "reply_target_url": url,
    }


def stored_reply_target(request: ExecutionRequest, work_item: WorkItem) -> ReplyTarget:
    """The request's stored reply target: the WorkItem's tracker issue, or a
    pull request on the repository frozen on the WorkItem."""

    if request.reply_target_kind == "issue":
        return ReplyTarget.on_issue(work_item.tracker_issue)
    assert request.reply_target_pr_number is not None
    pull_request = PullRequestRef(work_item.repository, request.reply_target_pr_number)
    if request.reply_target_kind == "review_thread":
        assert request.reply_target_comment_id is not None
        return ReplyTarget.on_thread(pull_request, request.reply_target_comment_id)
    return ReplyTarget.on_pull_request(pull_request)


def _first_line(objective: str | None) -> str | None:
    if not isinstance(objective, str) or not objective:
        return None
    return objective.split("\n", 1)[0]


def is_issue_follow_up(objective: str | None, *, issue_link: str, comment_prefix: str) -> bool:
    """Whether ``objective`` opens with a comment on the issue at ``issue_link``.

    ``comment_prefix`` is the tracker's comment fragment (`forges.trackers.comment_prefix`).
    The comment id is a positive 64-bit integer.
    """

    line = _first_line(objective)
    if line is None:
        return False
    match = re.fullmatch(
        rf"{re.escape(issue_link)}{re.escape(comment_prefix)}([1-9][0-9]{{0,18}})", line
    )
    return match is not None and int(match.group(1)) <= 2**63 - 1


def is_relabel_objective(objective: str | None, *, issue_link: str, comment_prefix: str) -> bool:
    """Whether ``objective`` opens with exactly the issue link: a relabel.

    ``comment_prefix`` is accepted so both matchers read one tracker's answer;
    a bare link carries no comment.
    """

    del comment_prefix
    return _first_line(objective) == issue_link
