"""Normalize untrusted review events without granting authority to their contents.

Only the authenticated ingress may call the later GitHub/lineage verifier. A
parsed event is not proof that its installation, author or PR is authorized.
URLs are reconstructed from validated identity, never used as fetch targets.
"""

import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .repo_full_name import InvalidRepoFullName, normalize_repo_full_name

_REVIEW_EVENTS = {
    "issue_comment": ("created", "comment", "issuecomment"),
    "pull_request_review_comment": ("created", "comment", "discussion_r"),
    "pull_request_review": ("submitted", "review", "pullrequestreview"),
}
_SHA = re.compile(r"[0-9a-fA-F]{40}")
_LOGIN = re.compile(r"[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*")
_MAX_FEEDBACK_LENGTH = 65536


class FeedbackIgnored(ValueError):
    """An observable refusal code that never includes webhook/model contents."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class FeedbackUnavailable(FeedbackIgnored):
    """Current authority could not be read; retry without executing a turn."""


@dataclass(frozen=True)
class UnverifiedFeedback:
    """Parsed sender claims; current GitHub truth and lineage still must agree."""

    delivery_id: uuid.UUID
    event: str
    installation_id: int
    repository_id: int
    repo_full_name: str
    pr_number: int
    feedback_id: int
    sender_id: int
    sender_login: str
    body: str
    url: str
    created_at: datetime
    head_sha: str | None
    commit_sha: str | None
    author_association: str
    path: str | None = None
    line: int | None = None
    review_id: int | None = None

    @property
    def event_id(self) -> str:
        # GitHub's delivery header is not covered by its body HMAC. A new
        # delivery ID for the same comment/review cannot create a second turn.
        # Installation identity is authorization provenance, not comment
        # identity: reinstalling the same App must not replay an old comment.
        identity = f"{self.repository_id}:{self.event}:{self.feedback_id}"
        return f"github-feedback-{uuid.uuid5(uuid.NAMESPACE_URL, identity)}"


def _object(value: Any, code: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise FeedbackIgnored(code)
    return value


def _positive(value: Any, code: str, maximum: int = 2**63 - 1) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 < value <= maximum:
        raise FeedbackIgnored(code)
    return value


def _instant(value: Any, code: str) -> datetime:
    if not isinstance(value, str):
        raise FeedbackIgnored(code)
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise FeedbackIgnored(code) from None
    if result.tzinfo is None:
        raise FeedbackIgnored(code)
    return result.astimezone(UTC)


def _human(value: Any) -> tuple[int, str]:
    user = _object(value, "non_human_sender")
    login = user.get("login")
    if (
        user.get("type") != "User"
        or not isinstance(login, str)
        or not 1 <= len(login) <= 39
        or _LOGIN.fullmatch(login) is None
        or login.casefold() == "ghost"
    ):
        raise FeedbackIgnored("non_human_sender")
    return _positive(user.get("id"), "non_human_sender"), login


def parse_feedback(event: str, payload: Any, delivery_id: str) -> UnverifiedFeedback:
    """Retain actionable human feedback claims or return an explicit refusal.

    Actions/fields follow the provider's webhook-event documentation:
    https://docs.github.com/en/webhooks/webhook-events-and-payloads
    ``issue_comment`` also covers ordinary issues, which cannot own a PR lineage.
    """

    definition = _REVIEW_EVENTS.get(event)
    if definition is None:
        raise FeedbackIgnored("unsupported_event")
    data = _object(payload, "invalid_payload")
    action, feedback_key, fragment_kind = definition
    if data.get("action") != action:
        raise FeedbackIgnored("unsupported_action")
    try:
        delivery = uuid.UUID(delivery_id)
    except (ValueError, TypeError, AttributeError):
        raise FeedbackIgnored("invalid_delivery") from None
    if str(delivery) != delivery_id.lower():
        raise FeedbackIgnored("invalid_delivery")
    installation = _object(data.get("installation"), "invalid_installation")
    installation_id = _positive(installation.get("id"), "invalid_installation")
    repository = _object(data.get("repository"), "invalid_repository")
    repository_id = _positive(repository.get("id"), "invalid_repository")
    repository_name = repository.get("full_name")
    if not isinstance(repository_name, str):
        raise FeedbackIgnored("invalid_repository")
    try:
        repo = normalize_repo_full_name(repository_name)
    except InvalidRepoFullName:
        raise FeedbackIgnored("invalid_repository") from None
    sender_id, sender_login = _human(data.get("sender"))
    feedback = _object(data.get(feedback_key), "invalid_feedback")
    author_id, author_login = _human(feedback.get("user"))
    if author_id != sender_id or author_login.casefold() != sender_login.casefold():
        raise FeedbackIgnored("sender_mismatch")
    if feedback.get("performed_via_github_app") is not None:
        raise FeedbackIgnored("app_authored")
    association = feedback.get("author_association")
    # CONTRIBUTOR is only a claim here. The shared truth verifier additionally
    # requires current App-proven write/admin permission before admitting it.
    if not isinstance(association, str) or association not in {
        "OWNER", "MEMBER", "COLLABORATOR", "CONTRIBUTOR"
    }:
        raise FeedbackIgnored("unauthorized_association")
    feedback_id = _positive(feedback.get("id"), "invalid_feedback")
    body = feedback.get("body")
    if not isinstance(body, str) or not body.strip():
        raise FeedbackIgnored("empty_feedback")
    if len(body) > _MAX_FEEDBACK_LENGTH:
        raise FeedbackIgnored("feedback_too_large")

    head_sha = None
    commit_sha = None
    path = None
    line = None
    review_id = None
    if event == "issue_comment":
        pr = _object(data.get("issue"), "not_pull_request")
        _object(pr.get("pull_request"), "not_pull_request")
    else:
        pr = _object(data.get("pull_request"), "not_pull_request")
        head = _object(pr.get("head"), "invalid_head")
        head_sha = head.get("sha")
        commit_sha = feedback.get("commit_id")
        if (
            not isinstance(head_sha, str)
            or _SHA.fullmatch(head_sha) is None
            or not isinstance(commit_sha, str)
            or _SHA.fullmatch(commit_sha) is None
        ):
            raise FeedbackIgnored("invalid_head")
        head_sha, commit_sha = head_sha.lower(), commit_sha.lower()
        if commit_sha != head_sha:
            raise FeedbackIgnored("stale_feedback_head")
    pr_number = _positive(pr.get("number"), "not_pull_request", 2**31 - 1)
    if pr.get("state") != "open" or pr.get("merged") is True:
        raise FeedbackIgnored("terminal_pull_request")
    if event == "pull_request_review":
        review_state = feedback.get("state")
        if not isinstance(review_state, str) or review_state.lower() not in {
            "commented",
            "changes_requested",
        }:
            raise FeedbackIgnored("non_actionable_review")
        created = _instant(feedback.get("submitted_at"), "invalid_feedback_time")
    else:
        created = _instant(feedback.get("created_at"), "invalid_feedback_time")
        updated = _instant(feedback.get("updated_at"), "invalid_feedback_time")
        if updated != created:
            raise FeedbackIgnored("edited_feedback")
    if event == "pull_request_review_comment":
        path = feedback.get("path")
        if not isinstance(path, str) or not path or len(path) > 4096:
            raise FeedbackIgnored("invalid_review_context")
        raw_line = feedback.get("line")
        if raw_line is not None:
            line = _positive(raw_line, "invalid_review_context")
        review_id = _positive(feedback.get("pull_request_review_id"), "invalid_review_context")
    separator = "" if fragment_kind == "discussion_r" else "-"
    url = f"https://github.com/{repo}/pull/{pr_number}#{fragment_kind}{separator}{feedback_id}"
    claimed_url = feedback.get("html_url")
    if not isinstance(claimed_url, str) or claimed_url.casefold() != url.casefold():
        raise FeedbackIgnored("invalid_feedback_url")
    return UnverifiedFeedback(
        delivery,
        event,
        installation_id,
        repository_id,
        repo,
        pr_number,
        feedback_id,
        sender_id,
        sender_login,
        body,
        url,
        created,
        head_sha,
        commit_sha,
        association,
        path,
        line,
        review_id,
    )
