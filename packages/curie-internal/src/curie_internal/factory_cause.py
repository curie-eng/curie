"""Operator-facing sentences for factory terminus causes.

The API status comment and the worker Slack progress lane both import
``cause_text`` from here. The worker cannot import the API package.
"""

from __future__ import annotations

# The operator-facing sentence for each terminus cause (#3073). The raw code
# still appears on the comment, but never as its headline.
_CAUSE_TEXT = {
    "model_credit_exhausted": (
        "the model provider refused the request because the account has run "
        "out of credits. Add credits or raise the key's limit, then retry."
    ),
    "model_usage_limited": (
        "the model provider's usage limit for this credential was reached; "
        "re-add the label after the limit resets."
    ),
    "model_credential_rejected": (
        "the model provider rejected the configured API key. Check the model "
        "credential, then retry."
    ),
    "model_rate_limited": (
        "the model provider kept rate limiting the request. Retry later or raise "
        "the provider limit."
    ),
    "model_error": "the model provider returned an error the run could not recover from.",
    "model_unreachable": (
        "the model provider could not be reached. Check the runner's network path "
        "to the model endpoint, then retry."
    ),
    "budget_exceeded": "the run reached a budget limit before it finished.",
    "runner_timeout": "the run took longer than its time limit.",
    "sandbox_terminated": (
        "the sandbox terminated before the run finished. Check the Kubernetes "
        "reason below, then retry after addressing the sandbox failure."
    ),
    "workspace_error": "the repository workspace could not be prepared for the run.",
    "history_capacity": (
        "conversation history capacity exceeded. Work may have happened. "
        "Inspect the result and retry."
    ),
    "runner_escalated": "the run stopped on an error and was handed to a person.",
    "pull_request_not_adopted": (
        "an earlier pull request on this issue could not be continued. "
        "A person should close or merge it, then re-add the label."
    ),
    "unclassified": (
        "the run failed and Curie could not name a more specific cause. "
        "Read the worker log for the provider message, then retry or hand it to a person."
    ),
    "max_turns": (
        "the run used its whole turn budget. Raise worker.workItemMaxTurns "
        "(CURIE_WORK_ITEM_MAX_TURNS) to allow more turns, then retry."
    ),
    "runner_failed": "the run ended without a result.",
    "approval_create_failed": (
        "the publication request was refused, so no pull request was opened. "
        "Read the details for the reason, fix it, then retry."
    ),
    "early_stop": "the agent stopped before doing any work on the issue.",
    "no_pull_request": "the run ended without publishing a pull request.",
    "revision_unchanged": (
        "the existing pull request is unchanged because this revision published nothing."
    ),
    "execution_deadline": "the run did not finish before its deadline.",
    "capacity_wait_expired": "sandbox admission did not complete before the wait expired.",
    "start_failed": "the sandbox did not start.",
    "owner_lost": "the worker running this request stopped responding.",
    # Personal Claude execution (#5377, ADR 0268 section 3).
    "enrollment_fenced": (
        "the run was stopped because its owner replaced or disconnected their personal "
        "Claude enrollment. The work is kept; the owner reconnects or replaces the "
        "enrollment, then resumes it."
    ),
    "authentication_required": (
        "the owner's personal Claude grant was missing or refused by the model provider. "
        "The work is kept; the owner reconnects or replaces the personal Claude "
        "enrollment, then resumes it."
    ),
    "issue_cancelled": "the request was cancelled.",
    "issue_deleted": "the tracker issue no longer exists.",
    "publication_denied": "a person denied the request to open the pull request.",
    "publication_expired": (
        "the request to open the pull request expired before anyone approved it."
    ),
    "publication_failed": (
        "publishing the pull request failed. A branch or pull request from this run may still "
        "exist; check the repository before retrying."
    ),
    "publication_unrecorded": (
        "the agent tried to publish its change, but no valid publication was recorded. "
        "The Details line below gives the reason; the pull request, if any, is unchanged."
    ),
    "ci_failed": (
        "the pull request's checks still failed after 3 rounds of fixes. The pull "
        "request stays open for a person."
    ),
    "ci_timeout": (
        "the pull request's checks did not finish before the CI wait ran out. The "
        "pull request stays open."
    ),
    "ci_unverified": (
        "the pull request's CI could not be verified, so the run did not complete. "
        "The Reason line below says why. The pull "
        "request stays open; check it yourself."
    ),
    "merge_conflict": (
        "the pull request has merge conflicts with its base branch, so GitHub ran "
        "no pull request checks. The pull request stays open; resolve the conflicts "
        "to continue."
    ),
    "pr_closed": "a person closed the pull request without merging it during the CI wait.",
    "ci_fix_unpublished": (
        "a CI fix round ended without pushing a fix. The pull request stays open."
    ),
    # Tracker-result (triage) runs (#5586).
    "tracker_result": "the investigation delivered its triage result to the issue.",
    "no_tracker_result": (
        "the investigation ended without submitting a result, so nothing was delivered "
        "to the issue."
    ),
    "tracker_result_undelivered": (
        "the investigation submitted a result, but it was not delivered to the issue "
        "before the run ended. Curie keeps trying until the delivery deadline."
    ),
}


def cause_text(cause: str) -> str:
    """A plain sentence for a terminus cause; unknown codes get a generic one."""

    return _CAUSE_TEXT.get(cause, "the run stopped for a reason Curie did not recognize.")
