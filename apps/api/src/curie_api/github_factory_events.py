"""Normalize untrusted GitHub issue events for factory intake.

The parser reuses the review ingress checks for humans, identifiers, and
repository names. A parsed notice is not authorization.
"""

import re
import uuid
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .github_review_events import (
    FeedbackIgnored,
    human_sender,
    payload_object,
    positive_identifier,
)
from .repo_full_name import InvalidRepoFullName, normalize_repo_full_name

_MAX_COMMENT_LENGTH = 65536
# The label prefix a ticket declares its base with (ADR 0186).
BASE_LABEL_PREFIX = "base:"


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
    # When the admitting label event happened, as GitHub's timeline reports it
    # (#4629). None when unknown, or for a notice that is not a label
    # admission; rediscovery treats unknown as an old trigger replay.
    label_event_at: datetime | None
    label: str | None = None
    comment_id: int | None = None
    comment_body: str | None = None
    label_event_id: int | None = None
    actor_refusal: str | None = None
    previously_admitted: bool = False
    # A created issue comment that is not a mention a human made (disposition
    # ``comment``, #5787): the code today's mention rules refuse it with. The
    # answer intake decides first; when the comment is not an answer, this is
    # the refusal the delivery records.
    comment_refusal: str | None = None

    @property
    def request_id(self) -> uuid.UUID:
        if self.disposition == "mention":
            identity = f"https://github.com/factory/mention/{self.repository_id}/{self.comment_id}"
        elif self.disposition == "admit" and self.label_event_id is not None:
            # The timeline event is the admission identity, shared by poll and
            # webhook. The webhook receipt stays the delivery header.
            identity = (
                f"https://github.com/factory/label/{self.repository_id}/"
                f"{self.issue_number}/{self.label_event_id}"
            )
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


def mentions_factory(body: str, mention: str, app_slug: str | None) -> bool:
    """True when body names the configured mention or the App's own slug.

    ``app_slug`` is the App login from ``GitHubCredentials.app_slug``. None
    means that login could not be read, and only the configured mention matches.
    ``[`` is a token boundary, so ``@slug[bot]`` matches the slug.

    A factory status comment carries ``<!-- curie-execution-request:`` and the
    ask line, which names the mention. That comment is the factory speaking,
    not a new ask.
    """

    if "<!-- curie-execution-request:" in body:
        return False
    if mentions_login(body, mention):
        return True
    if app_slug is None or app_slug.casefold() == mention.casefold():
        return False
    return mentions_login(body, app_slug)


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


def event_repository(payload: Any) -> str | None:
    """The normalized repository a factory event names, or None when it names
    none readable. The trigger labels bound on it are what `parse_factory_event`
    judges a label event against; a malformed payload is refused there."""

    if not isinstance(payload, dict):
        return None
    try:
        return _repository(payload)[1]
    except FeedbackIgnored:
        return None


def parse_factory_event(
    event: str,
    payload: Any,
    delivery_id: str,
    *,
    labels: Collection[str],
    mention: str,
    app_slug: str | None,
) -> FactoryNotice:
    """Keep an admission, cancellation, explicit mention, or created issue comment.
    Refuse everything else.

    A created issue comment that is not a mention is kept as a ``comment``
    notice with the refusal it gets when it is not an answer (#5787); an edited
    or deleted comment stays ``unsupported_action`` and never answers.

    ``labels`` are the effective trigger labels bound on the event's repository
    (#4361 decision 15). Adding one of them is an admission. Removing any label
    that is not a ``base:`` label is a cancellation candidate, whether or not a
    surface still binds it: whether it admitted the issue's WorkItem is the
    WorkItem's stored label to decide (decision 10, ADR 0161), never a binding.

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
    action = data.get("action")

    if event == "issues":
        label = data.get("label")
        label_name = label.get("name") if isinstance(label, dict) else None
        canonical_admission = (
            action == "labeled"
            and isinstance(label_name, str)
            and not label_name.startswith(BASE_LABEL_PREFIX)
            and label_name in labels
        )
        sender_id, sender_login = 0, ""
        if not canonical_admission:
            sender = payload_object(data.get("sender"), "non_human_sender")
            _reject_app(sender, None)
            sender_id, sender_login = human_sender(sender)
        if action in {"labeled", "unlabeled"}:
            label_obj = payload_object(data.get("label"), "invalid_label")
            name = label_obj.get("name")
            if not isinstance(name, str):
                raise FactoryRefused("unrelated_label")
            if name.startswith(BASE_LABEL_PREFIX):
                # A base label change only records disagreement (ADR 0186).
                disposition = "base_label"
            elif action == "unlabeled":
                disposition = "cancel"
            elif name in labels:
                disposition = "admit"
            else:
                raise FactoryRefused("unrelated_label")
            return FactoryNotice(
                delivery,
                event,
                action,
                disposition,
                installation_id,
                repository_id,
                repo,
                issue_number,
                sender_id,
                sender_login,
                label_event_at=None,
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
                label_event_at=None,
            )
        raise FactoryRefused("unsupported_action")

    if event != "issue_comment":
        raise FactoryRefused("unsupported_event")
    if action != "created":
        raise FactoryRefused("unsupported_action")
    return _comment_notice(
        data,
        delivery,
        installation_id,
        repository_id,
        repo,
        issue_number,
        mention=mention,
        app_slug=app_slug,
    )


def _comment_notice(
    data: dict[str, Any],
    delivery: uuid.UUID,
    installation_id: int,
    repository_id: int,
    repo: str,
    issue_number: int,
    *,
    mention: str,
    app_slug: str | None,
) -> FactoryNotice:
    """A created issue comment: a ``mention`` notice, or a ``comment`` notice
    carrying the refusal the mention rules give it (#5787).

    Whether a comment answers the factory's question depends on who GitHub
    says wrote it, which the answer intake reads through the App, so the
    payload's author claims no longer end the delivery here. The checks run in
    today's order and the first author refusal is the one kept; a structural
    refusal after it still answers with that author refusal, exactly as the
    parser always did. https://docs.github.com/en/webhooks/webhook-events-and-payloads#issue_comment
    """

    refusal: str | None = None

    def refuse(code: str) -> None:
        nonlocal refusal
        if refusal is None:
            refusal = code

    def structural(code: str) -> FactoryRefused:
        return FactoryRefused(refusal or code)

    sender = data.get("sender")
    comment = data.get("comment")
    if not isinstance(sender, dict):
        refuse("non_human_sender")
    if not isinstance(comment, dict):
        raise structural("invalid_comment")
    sender_id, sender_login = 0, ""
    if isinstance(sender, dict):
        try:
            _reject_app(sender, comment)
            comment_user = payload_object(comment.get("user"), "non_human_sender")
            if comment_user.get("type") == "Bot":
                raise FactoryRefused("app_authored")
            claimed_id, claimed_login = human_sender(sender)
            author_id, author_login = human_sender(comment_user)
            if author_id != claimed_id or author_login.casefold() != claimed_login.casefold():
                raise FactoryRefused("sender_mismatch")
            sender_id, sender_login = claimed_id, claimed_login
        except FeedbackIgnored as exc:
            refuse(exc.code)
    body = comment.get("body")
    if not isinstance(body, str):
        raise structural("invalid_comment")
    if len(body) > _MAX_COMMENT_LENGTH:
        raise structural("comment_too_large")
    if not body.strip() or not mentions_factory(body, mention, app_slug):
        refuse("ordinary_comment")
    try:
        comment_id = positive_identifier(comment.get("id"), "invalid_comment")
    except FeedbackIgnored as exc:
        raise structural(exc.code) from None
    return FactoryNotice(
        delivery,
        "issue_comment",
        "created",
        "mention" if refusal is None else "comment",
        installation_id,
        repository_id,
        repo,
        issue_number,
        sender_id,
        sender_login,
        label_event_at=None,
        comment_id=comment_id,
        comment_body=body,
        comment_refusal=refusal,
    )
