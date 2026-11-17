"""The marker every pull-request follow-up objective carries.

The dark factory review gate refuses an empty diff review only when the
opening prompt contains this line. Slack steering and both review-feedback
arms emit it through ``with_pull_request_follow_up_marker``. The gate keeps
the same literal in ``examples/dark-factory/hooks/review_gate.py`` because
that hook runs on the sandbox interpreter and cannot import this module.
The regression that builds each objective keeps the two copies equal.
"""

from __future__ import annotations

PULL_REQUEST_FOLLOW_UP_MARKER = "Curie pull request follow-up."


def with_pull_request_follow_up_marker(objective: str) -> str:
    """The objective with the marker on its own line after the opening URL.

    The first line stays the pull request URL. Callers that read that line
    (issue follow-up and relabel matching) are unchanged.
    """

    url, _separator, rest = objective.partition("\n")
    body = rest.lstrip("\n")
    if body == PULL_REQUEST_FOLLOW_UP_MARKER or body.startswith(
        f"{PULL_REQUEST_FOLLOW_UP_MARKER}\n"
    ):
        return objective
    if body:
        return f"{url}\n\n{PULL_REQUEST_FOLLOW_UP_MARKER}\n{body}"
    return f"{url}\n\n{PULL_REQUEST_FOLLOW_UP_MARKER}\n"
