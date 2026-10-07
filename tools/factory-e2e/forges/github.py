"""The GitHub entry of the forge-neutral factory loop (`github-loop`).

Wraps the live Preflight: the ticket is an issue labelled at creation, the
change is the pull request the WorkItem records, CI is GitHub check runs and
commit statuses, the review is a pull request review, and the merge is the
pull request merge API, every write made as the test actor on the dedicated
test GitHub App's fixture repository.

Red CI is set deliberately: the test actor posts one failing commit status
(LOOP_GATE_CONTEXT) on the pull request's first head as soon as the WorkItem
records the pull request. The factory CI gate reads commit statuses as CI, so
the first head is red whatever the fixture's own workflow reports, and the
gate sends the failure to the agent as a CI fix round. The status names a
small change to push. A later head carries no such status, so the fixture's
own checks decide whether the fix is green.
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

import factory_e2e as fe

LOOP_GATE_CONTEXT = "factory-e2e/loop-gate"
# A commit status description holds at most 140 characters.
LOOP_GATE_DESCRIPTION = (
    "Loop gate: add a one line entry for this change to CHANGELOG.md and push it to this branch."
)
_FAILING_CONCLUSIONS = frozenset(
    {"failure", "timed_out", "cancelled", "action_required", "startup_failure", "stale"}
)
_PASSING_CONCLUSIONS = frozenset({"success", "neutral", "skipped"})
MERGE_METHODS = ("merge", "squash", "rebase")


def github_ci_state(
    check_runs: Sequence[Mapping[str, Any]], statuses: Sequence[Mapping[str, Any]]
) -> str:
    """``failing``, ``pending``, ``passing`` or ``none`` for one head. Pure.

    ``statuses`` is the per-context list of the combined status, newest per
    context, as `GET /commits/{sha}/status` returns it.
    """

    if not check_runs and not statuses:
        return "none"
    if any(
        run.get("status") == "completed" and run.get("conclusion") in _FAILING_CONCLUSIONS
        for run in check_runs
    ) or any(item.get("state") in ("failure", "error") for item in statuses):
        return "failing"
    if any(
        run.get("status") != "completed" or run.get("conclusion") not in _PASSING_CONCLUSIONS
        for run in check_runs
    ) or any(item.get("state") != "success" for item in statuses):
        return "pending"
    return "passing"


def match_review_delivery(
    deliveries: list[dict[str, Any]], *, review_id: int, repo: str
) -> dict[str, Any] | None:
    """The newest `pull_request_review.submitted` delivery for this review. Pure."""

    found = None
    for delivery in deliveries:
        if delivery.get("event") != "pull_request_review" or delivery.get("action") != "submitted":
            continue
        payload = (delivery.get("request") or {}).get("payload") or {}
        review = payload.get("review") or {}
        repository = payload.get("repository") or {}
        if review.get("id") == review_id and repository.get("full_name") == repo:
            found = delivery
    return found


class GitHubLoop:
    forge = "github"

    def __init__(self, p: fe.Preflight) -> None:
        self.p = p
        self.repo = f"/repos/{p.config.repo}"

    def open_ticket(self, title: str, body: str) -> int:
        self.p.issue_spec = (title, body)
        return self.p.open_labelled_issue()

    def mark_ticket(self, ticket: int, since: float) -> str:
        # The issue is labelled when it is opened; marking is proving admission.
        self.p.assert_admission(ticket, since)
        return str(self.p.evidence["work_item_id"])

    def await_change(self, work_item_id: str) -> fe.ChangeRef | None:
        return fe.await_work_item_change(self.p, work_item_id)

    def change_head(self, change: fe.ChangeRef) -> tuple[str | None, int | None]:
        return self.p.pr_head(change.number)

    def set_ci_red(self, sha: str) -> dict[str, Any]:
        status, body = self.p.as_actor(
            "POST",
            f"{self.repo}/statuses/{sha}",
            {
                "state": "failure",
                "context": LOOP_GATE_CONTEXT,
                "description": LOOP_GATE_DESCRIPTION,
            },
        )
        if status != 201 or not isinstance(body, dict):
            raise fe.PreflightFailed(f"setting the red loop gate status failed (HTTP {status})")
        return {"sha": sha, "context": LOOP_GATE_CONTEXT, "state": body.get("state")}

    def ci_state(self, sha: str) -> dict[str, Any]:
        status, runs = self.p.as_actor("GET", f"{self.repo}/commits/{sha}/check-runs?per_page=100")
        check_runs = runs.get("check_runs") if status == 200 and isinstance(runs, dict) else None
        status, combined = self.p.as_actor("GET", f"{self.repo}/commits/{sha}/status")
        statuses = (
            combined.get("statuses") if status == 200 and isinstance(combined, dict) else None
        )
        if not isinstance(check_runs, list) or not isinstance(statuses, list):
            return {"sha": sha, "state": "unreadable"}
        return {
            "sha": sha,
            "state": github_ci_state(check_runs, statuses),
            "check_runs": [
                {
                    "name": run.get("name"),
                    "status": run.get("status"),
                    "conclusion": run.get("conclusion"),
                }
                for run in check_runs
            ],
            "statuses": [{"context": s.get("context"), "state": s.get("state")} for s in statuses],
        }

    def post_review(self, change: fe.ChangeRef, text: str) -> dict[str, Any]:
        since = time.time()
        body = fe.revision_comment_text(text, self.p.config.mention)
        status, review = self.p.as_actor(
            "POST",
            f"{self.repo}/pulls/{change.number}/reviews",
            {"body": body, "event": "COMMENT"},
        )
        if status != 200 or not isinstance(review, dict) or not review.get("id"):
            raise fe.PreflightFailed(f"posting the review failed (HTTP {status})")
        review_id = int(review["id"])
        delivery = self.p.await_delivery(
            since,
            event="pull_request_review",
            action="submitted",
            match=lambda details: match_review_delivery(
                details, review_id=review_id, repo=self.p.config.repo
            ),
            what=f"the delivery of review {review_id}",
        )
        return {
            "id": review_id,
            "url": review.get("html_url"),
            "delivery_id": delivery.get("guid"),
            "delivery_status_code": delivery.get("status_code"),
            "delivery_api_status": fe.delivery_api_status(delivery),
        }

    def merge(self, change: fe.ChangeRef) -> dict[str, Any]:
        """Merge as the test actor with the first method the repository allows."""

        status = 0
        body: Any = None
        for method in MERGE_METHODS:
            status, body = self.p.as_actor(
                "PUT", f"{self.repo}/pulls/{change.number}/merge", {"merge_method": method}
            )
            if status != 405:
                break
        merged = isinstance(body, dict) and body.get("merged") is True
        sha = body.get("sha") if isinstance(body, dict) else None
        return {"status_code": status, "merged": merged, "method": method, "sha": sha}

    def change_merged(self, change: fe.ChangeRef) -> bool | None:
        status, body = self.p.as_actor("GET", f"{self.repo}/pulls/{change.number}")
        if status != 200 or not isinstance(body, dict):
            return None
        return body.get("merged") is True

    def changes_opened(self) -> list[int]:
        return fe._scenario_pr_numbers(self.p)

    def default_branch_head(self) -> str:
        return self.p.default_branch_head()


fe.register_forge(fe.ForgeEntry(name="github", scenario="github-loop", driver=GitHubLoop))
