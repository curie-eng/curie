"""Normalize untrusted GitHub issue events for factory intake.

The parser reuses the review ingress checks for humans, identifiers, and
repository names. A parsed notice is not authorization.
"""

import re
import uuid
from dataclasses import dataclass
from typing import Any

from .github_review_events import (
    FeedbackIgnored,
    human_sender,
    payload_object,
    positive_identifier,
)
from .repo_full_name import InvalidRepoFullName, normalize_repo_full_name

_MAX_COMMENT_LENGTH = 65536


class FactoryRefused(FeedbackIgnored):
    """Payload-free factory refusal."""


@dataclass(frozen=True)
class FactoryNotice:
    """Claims taken from one signed issue event. GitHub must still confirm them."""

    delivery_id: uuid.UUID
    event: str
    action: str
    disposition: str
    installation_id: int
    repository_id: int
    repo_full_name: str
    issue_number: int
    sender_id: int
    sender_login: str
    label: str | None = None
    comment_id: int | None = None
    comment_body: str | None = None

    @property
    def request_id(self) -> uuid.UUID:
        if self.disposition == "mention":
            identity = f"https://github.com/factory/mention/{self.repository_id}/{self.comment_id}"
        else:
            # Each labeled delivery is its own request, so a relabel starts a
            # new run. Redelivery of the same delivery is deduped upstream.
            identity = (
                f"https://github.com/factory/label/{self.repository_id}/"
                f"{self.issue_number}/{self.delivery_id}"
            )
        return uuid.uuid5(uuid.NAMESPACE_URL, identity)


def is_plain_issue(payload: Any) -> bool:
    """True when the event's issue is not a pull request."""

    if not isinstance(payload, dict):
        return False
    issue = payload.get("issue")
    return isinstance(issue, dict) and "pull_request" not in issue


def mentions_login(body: str, login: str) -> bool:
    """True when body contains one explicit @login token, not a suffix or email."""

    pattern = rf"(?<![A-Za-z0-9-])@{re.escape(login)}(?![A-Za-z0-9-])"
    return re.search(pattern, body, flags=re.IGNORECASE) is not None


def _delivery(delivery_id: str) -> uuid.UUID:
    try:
        delivery = uuid.UUID(delivery_id)
    except (ValueError, TypeError, AttributeError):
        raise FactoryRefused("invalid_delivery") from None
    if str(delivery) != delivery_id.lower():
        raise FactoryRefused("invalid_delivery")
    return delivery


def _reject_app(sender: dict[str, Any], source: dict[str, Any] | None) -> None:
    if sender.get("type") == "Bot":
        raise FactoryRefused("app_authored")
    if source is not None and source.get("performed_via_github_app") is not None:
        raise FactoryRefused("app_authored")


def _repository(data: dict[str, Any]) -> tuple[int, str, int]:
    installation = payload_object(data.get("installation"), "invalid_installation")
    installation_id = positive_identifier(installation.get("id"), "invalid_installation")
    repository = payload_object(data.get("repository"), "invalid_repository")
    repository_id = positive_identifier(repository.get("id"), "invalid_repository")
    repository_name = repository.get("full_name")
    if not isinstance(repository_name, str):
        raise FactoryRefused("invalid_repository")
    try:
        repo = normalize_repo_full_name(repository_name)
    except InvalidRepoFullName:
        raise FactoryRefused("invalid_repository") from None
    return installation_id, repo, repository_id


def parse_factory_event(
    event: str,
    payload: Any,
    delivery_id: str,
    *,
    label: str,
    mention: str,
) -> FactoryNotice:
    """Keep an admission, cancellation, or explicit mention. Refuse everything else.

    Actions and fields follow GitHub's webhook catalog:
    https://docs.github.com/en/webhooks/webhook-events-and-payloads
    """

    data = payload_object(payload, "invalid_payload")
    delivery = _delivery(delivery_id)
    installation_id, repo, repository_id = _repository(data)
    issue = payload_object(data.get("issue"), "invalid_issue")
    if "pull_request" in issue:
        raise FactoryRefused("pull_request_issue")
    issue_number = positive_identifier(issue.get("number"), "invalid_issue", 2**31 - 1)
    sender = payload_object(data.get("sender"), "non_human_sender")
    action = data.get("action")

    if event == "issues":
        _reject_app(sender, None)
        sender_id, sender_login = human_sender(sender)
        if action in {"labeled", "unlabeled"}:
            label_obj = payload_object(data.get("label"), "invalid_label")
            name = label_obj.get("name")
            if not isinstance(name, str) or name != label:
                raise FactoryRefused("unrelated_label")
            return FactoryNotice(
                delivery,
                event,
                action,
                "admit" if action == "labeled" else "cancel",
                installation_id,
                repository_id,
                repo,
                issue_number,
                sender_id,
                sender_login,
                label=name,
            )
        if action == "closed":
            return FactoryNotice(
                delivery,
                event,
                action,
                "cancel",
                installation_id,
                repository_id,
                repo,
                issue_number,
                sender_id,
                sender_login,
            )
        raise FactoryRefused("unsupported_action")

    if event != "issue_comment":
        raise FactoryRefused("unsupported_event")
    if action != "created":
        raise FactoryRefused("unsupported_action")
    comment = payload_object(data.get("comment"), "invalid_comment")
    _reject_app(sender, comment)
    comment_user = payload_object(comment.get("user"), "non_human_sender")
    if comment_user.get("type") == "Bot":
        raise FactoryRefused("app_authored")
    sender_id, sender_login = human_sender(sender)
    author_id, author_login = human_sender(comment_user)
    if author_id != sender_id or author_login.casefold() != sender_login.casefold():
        raise FactoryRefused("sender_mismatch")
    body = comment.get("body")
    if not isinstance(body, str):
        raise FactoryRefused("invalid_comment")
    if len(body) > _MAX_COMMENT_LENGTH:
        raise FactoryRefused("comment_too_large")
    if not body.strip() or not mentions_login(body, mention):
        raise FactoryRefused("ordinary_comment")
    comment_id = positive_identifier(comment.get("id"), "invalid_comment")
    return FactoryNotice(
        delivery,
        event,
        "created",
        "mention",
        installation_id,
        repository_id,
        repo,
        issue_number,
        sender_id,
        sender_login,
        comment_id=comment_id,
        comment_body=body,
    )
