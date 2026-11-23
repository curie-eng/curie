"""GitHub as the factory's Tracker (ADR 0197, "Two ports" item 1).

One instance serves one repository: its issues are the tickets, the
repository id is the scope of every `TrackerIssueRef`, and the issue number is
the issue id. The adapter owns every GitHub REST call the factory's intake,
admission, ticket read and state labels make. Callers mint the App
installation token as before and hand it in as a `TokenSource`.

Two groups of methods live here:

1. The Tracker port. ``poll_marked`` reads the repository's issue events and
   issue comments since an opaque cursor, so a relabel is two markings and a
   repeated poll of one cursor is the same page.
2. The GitHub intake reads the production poll pass, the missed-label
   reconciler and the signed webhook make today (open labeled issues with an
   ETag, one issue's events, a conditional issue read, the webhook notice
   verification). They keep today's requests, refusal codes and ETag use
   exactly; moving those callers onto ``poll_marked`` would change what a pass
   reads, so it waits for the config switch (#3831 commit 10).

Endpoints:
https://docs.github.com/en/rest/issues/events#list-issue-events-for-a-repository
https://docs.github.com/en/rest/issues/events#list-issue-events
https://docs.github.com/en/rest/issues/comments#list-issue-comments-for-a-repository
https://docs.github.com/en/rest/issues/issues#list-repository-issues
https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user
https://docs.github.com/en/rest/issues/labels#add-labels-to-an-issue
https://docs.github.com/en/rest/issues/labels#remove-a-label-from-an-issue
https://docs.github.com/en/rest/issues/issues#create-an-issue
https://docs.github.com/en/rest/issues/issues#get-an-issue
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit

import httpx

from curie_api.forges.capabilities import TRACKER_OPERATIONS, Operation, Support
from curie_api.forges.errors import (
    NotFound,
    Unauthorized,
    Unavailable,
    Unsupported,
)
from curie_api.forges.github.comments import GitHubMarkedComments, TokenSource
from curie_api.forges.github.marked_comments import REFUSED_STATUSES, github_request
from curie_api.forges.github.review_polling import human_actor, trailing_id
from curie_api.forges.github.transport import (
    PollUnavailable,
    get_all,
    get_github_json,
    github_headers,
    list_pages,
    parse_time,
    repository_identity_matches,
)
from curie_api.forges.github.transport import Unavailable as TransportUnavailable
from curie_api.forges.ports import TrackerFacts
from curie_api.forges.types import (
    GITHUB,
    Actor,
    Disposition,
    MarkedNotice,
    NoticePage,
    PullRequest,
    RepositoryRef,
    SourceContent,
    TicketLabels,
    TrackerComment,
    TrackerIssueRef,
)
from curie_api.github_factory_events import FactoryNotice, FactoryRefused, mentions_factory
from curie_api.github_review_events import FeedbackIgnored, FeedbackUnavailable, label_actor
from curie_api.github_review_truth import verify_sender_write_permission
from curie_api.repo_full_name import repo_url_path

# State label refusals have always been logged under the status comment pass.
logger = logging.getLogger("curie_api.factory_notices")

_ISSUE_READ_PROVIDER_TIMEOUT_SECONDS = 5.0
_ISSUE_READ_MAX_RESPONSE_BYTES = 2_097_152
_ISSUE_READ_COMMENTS_PER_PAGE = 100
# Ten pages is a thousand comments. A longer thread reads the ten pages ending
# at its last page, so the newest comments (the ones that carry the latest
# answer) are kept, and says the earlier ones were not read.
_ISSUE_READ_MAX_COMMENT_PAGES = 10
# A port listing longer than this many pages is not trusted to be complete.
_MAX_PAGES = 50


_LINK_LAST = re.compile(r'<([^>]*)>\s*;\s*rel="last"')


def _last_page(headers: httpx.Headers) -> int | None:
    """The last page number from the `Link` header's `rel="last"` entry.

    https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api
    GitHub sends `rel="last"` whenever there is a following page and omits the
    header on a single page. None means a `rel="next"` without a `rel="last"`,
    which the read follows forward instead.
    """

    link = headers.get("link", "")
    found = _LINK_LAST.search(link)
    if found is None:
        return None if 'rel="next"' in link else 1
    values = parse_qs(urlsplit(found.group(1)).query).get("page", [])
    if len(values) != 1 or not values[0].isdecimal() or int(values[0]) < 1:
        raise Unavailable("issue comments")
    return int(values[0])


@dataclass(frozen=True)
class IssueComment:
    author: str | None
    created_at: str | None
    body: str


@dataclass(frozen=True)
class IssueContent:
    title: str
    body: str
    state: str | None
    author: str | None
    comments: list[IssueComment]
    earlier_comments_omitted: bool


@dataclass(frozen=True)
class IssueFacts:
    """What GitHub confirmed about a notice's issue and repository."""

    labels: set[str]
    default_branch: str | None


@dataclass(frozen=True)
class IssueState:
    """One issue as GitHub answers it now (#5380): its state, its label names,
    and whether the number is a pull request's."""

    state: str | None
    labels: frozenset[str]
    pull_request: bool


def last_label_event(events: list[Any], label: str) -> dict[str, Any] | None:
    """The newest ``labeled`` event for this label, or None."""

    found: dict[str, Any] | None = None
    for event in events:
        if (
            isinstance(event, dict)
            and event.get("event") == "labeled"
            and isinstance(event.get("label"), dict)
            and event["label"].get("name") == label
        ):
            found = event
    return found


def last_event_of_kind(
    events: list[Any], kind: str, *, label: str | None = None
) -> dict[str, Any] | None:
    """The newest event of ``kind``, for ``label`` when one is given."""

    found: dict[str, Any] | None = None
    for event in events:
        if not isinstance(event, dict) or event.get("event") != kind:
            continue
        if label is not None:
            raw = event.get("label")
            if not isinstance(raw, dict) or raw.get("name") != label:
                continue
        found = event
    return found


def label_names(issue: dict[str, Any]) -> set[str] | None:
    """The issue's label names, or None when the payload is malformed."""

    labels = issue.get("labels")
    if not isinstance(labels, list):
        return None
    names: set[str] = set()
    for label in labels:
        if not isinstance(label, dict) or not isinstance(label.get("name"), str):
            return None
        names.add(label["name"])
    return names


async def read_repository(
    client: httpx.AsyncClient, *, api: str, repo_full_name: str, token: str
) -> dict[str, Any]:
    """The repository object; refusals are `FeedbackUnavailable` or `FeedbackIgnored`."""

    return await get_github_json(
        client,
        api=api.rstrip("/"),
        token=token,
        path=f"/repos/{repo_url_path(repo_full_name)}",
        refusal="repository_unavailable",
    )


def _status_error(status: int, what: str) -> Exception:
    if status == 401:
        return Unauthorized(what)
    if status in {404, 410}:
        return NotFound(what)
    return Unavailable(what)


def _positive_int(value: Any) -> int | None:
    return value if type(value) is int and value > 0 else None


def _parse_cursor(cursor: str | None) -> tuple[int, int]:
    if cursor is None:
        return 0, 0
    events, sep, comments = cursor.partition(":")
    if not sep or not events.isdigit() or not comments.isdigit():
        raise ValueError("a GitHub tracker cursor is <event id>:<comment id>")
    return int(events), int(comments)


def issue_url(html_base: str, repo_full_name: str, issue: TrackerIssueRef) -> str:
    """The web link to a GitHub issue on ``repo_full_name``. Makes no request."""

    if issue.kind != GITHUB or not issue.issue_id.isascii() or not issue.issue_id.isdigit():
        raise NotFound("issue")
    return f"{html_base.rstrip('/')}/{repo_full_name}/issues/{issue.issue_id}"


class GitHubTracker:
    """The factory's tracker on one GitHub repository."""

    kind = GITHUB
    capabilities: Mapping[Operation, Support] = {
        **dict.fromkeys(TRACKER_OPERATIONS, Support.SUPPORTED),
        # The closing reference in the pull request body already links it.
        Operation.LINK_PULL_REQUEST: Support.NOOP,
        # The factory reads no GitHub issue dependencies yet (ADR-0165 is a Draft).
        Operation.DEPENDENCIES: Support.NOOP,
        # GitHub start authority is the repository write check, never a group.
        Operation.GROUP_MEMBERSHIP: Support.UNSUPPORTED,
    }

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        api: str,
        html_base: str,
        repo_full_name: str,
        repository_id: int,
        token: TokenSource,
        label: str,
        mention: str,
        app_id: str,
        own_login: TokenSource,
        page_size: int = 100,
    ) -> None:
        """``own_login`` answers the login this adapter's token writes as: the
        App's bot user ``<slug>[bot]`` (`GitHubTrackerConnection` reads the slug
        under the App JWT)."""

        if page_size < 1:
            raise ValueError("page_size must be positive")
        self._client = client
        self._api = api.rstrip("/")
        self._html_base = html_base
        self.host = urlsplit(html_base).netloc.lower()
        self.repo_full_name = repo_full_name
        self.repository_id = repository_id
        self._repo_path = f"/repos/{repo_url_path(repo_full_name)}"
        self._token = token
        self._own_login = own_login
        self.label = label
        self.mention = mention
        self.app_id = app_id
        self._page_size = page_size
        self._comments = GitHubMarkedComments(
            client,
            api=api,
            host=self.host,
            repo_full_name=repo_full_name,
            repository_id=repository_id,
            token=token,
            app_id=app_id,
        )

    async def app_slug(self) -> str | None:
        """The App login this tracker writes as, or None when it cannot be read.

        ``own_login`` is ``<slug>[bot]``. A login that is not that shape is not
        an App slug, and a lookup failure leaves the configured mention as the
        only match.
        """

        try:
            login = await self._own_login()
        except Unavailable:
            return None
        if not isinstance(login, str) or not login.endswith("[bot]"):
            return None
        slug = login[: -len("[bot]")]
        return slug or None

    @property
    def marked_comments(self) -> GitHubMarkedComments:
        return self._comments

    @property
    def facts(self) -> TrackerFacts:
        return self

    @property
    def repository(self) -> RepositoryRef:
        return RepositoryRef(self.kind, self.host, str(self.repository_id), self.repo_full_name)

    def facts_comment(self, value: Mapping[str, Any]) -> TrackerComment | None:
        number = trailing_id(value.get("issue_url"))
        comment_id, body = value.get("id"), value.get("body")
        if (
            number is None
            or type(comment_id) is not int
            or comment_id <= 0
            or not isinstance(body, str)
        ):
            return None
        try:
            actor_id, login = label_actor(
                {
                    "actor": value.get("user"),
                    "performed_via_github_app": value.get("performed_via_github_app"),
                },
                app_id=self.app_id,
            )
            actor = Actor(str(actor_id), login)
        except FeedbackIgnored:
            actor = None
        return TrackerComment(
            comment_id,
            self.issue(number),
            body,
            actor,
            value.get("performed_via_github_app") is not None,
            parse_time(value.get("updated_at")),
        )

    async def read_comment(self, comment_id: int) -> TrackerComment:
        """Re-read request content and provenance through the installation credential."""
        comment = self.facts_comment(await self.issue_comment(comment_id))
        if comment is None:
            raise Unavailable("comment")
        return comment

    async def issue_comment(self, comment_id: int) -> dict[str, Any]:
        """One issue comment as GitHub answers it now, through the installation
        credential: its ``user``, ``performed_via_github_app``, ``issue_url``
        and ``created_at`` are the factory's own read, not a payload's claim
        (#5787). A 404 is `NotFound`; any other failure, or a payload that is
        not this comment, is a `ForgeError`.

        https://docs.github.com/en/rest/issues/comments#get-an-issue-comment
        """

        value = await self._get(f"{self._repo_path}/issues/comments/{comment_id}")
        if not isinstance(value, dict) or value.get("id") != comment_id:
            raise Unavailable("comment")
        return value

    async def facts_issue_is_plain(self, ref: TrackerIssueRef) -> bool:
        number = self._number(ref)
        try:
            issue = await self._get(f"{self._repo_path}/issues/{number}")
        except (NotFound, Unauthorized):
            raise Unavailable("issue") from None
        if (
            not isinstance(issue, dict)
            or type(issue.get("number")) is not int
            or issue["number"] != number
        ):
            raise Unavailable("issue")
        return "pull_request" not in issue

    async def facts_writer(self, comment: TrackerComment) -> Actor | None:
        """Reuse admission's immutable-user effective write permission check."""
        actor = comment.actor
        if actor is None:
            return None
        self._number(comment.issue)
        try:
            await verify_sender_write_permission(
                self._client,
                api=self._api,
                token=await self._token(),
                repo_path=self._repo_path,
                sender_id=int(actor.id),
                sender_login=actor.login,
            )
        except FeedbackIgnored as exc:
            if exc.code == "sender_permission_unavailable":
                raise Unavailable("actor_unreadable") from None
            return None
        return actor

    def issue(self, number: int) -> TrackerIssueRef:
        return TrackerIssueRef(GITHUB, self.host, str(self.repository_id), str(number))

    def _number(self, issue: TrackerIssueRef) -> int:
        if (
            issue.kind != GITHUB
            or issue.host != self.host
            or issue.scope_id != str(self.repository_id)
            or not issue.issue_id.isdigit()
        ):
            raise NotFound("issue")
        return int(issue.issue_id)

    # HTTP ---------------------------------------------------------------

    async def _get(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        token = await self._token()
        try:
            response = await self._client.get(
                f"{self._api}{path}",
                params=dict(params or {}),
                headers=github_headers(token),
                follow_redirects=False,
            )
        except httpx.HTTPError:
            raise Unavailable(path) from None
        if response.status_code != 200:
            raise _status_error(response.status_code, path)
        try:
            return response.json()
        except ValueError:
            raise Unavailable(path) from None

    async def _pages(
        self, path: str, params: Mapping[str, Any], *, newer_than: int | None = None
    ) -> list[Any]:
        """Every page of one listing, or `Unavailable`.

        With ``newer_than`` the listing is newest first and stops at the first
        item whose id is not newer; nothing older can follow.
        """

        items: list[Any] = []
        for page in range(1, _MAX_PAGES + 1):
            batch = await self._get(path, {**params, "per_page": self._page_size, "page": page})
            if not isinstance(batch, list):
                raise Unavailable(path)
            if newer_than is not None:
                for item in batch:
                    item_id = _positive_int(item.get("id")) if isinstance(item, dict) else None
                    if item_id is not None and item_id <= newer_than:
                        return items
                    items.append(item)
            else:
                items.extend(batch)
            if len(batch) < self._page_size:
                return items
        raise Unavailable(path)

    async def _issue(self, number: int) -> dict[str, Any] | None:
        """The issue, or None when it is gone or is a pull request."""

        try:
            issue = await self._get(f"{self._repo_path}/issues/{number}")
        except NotFound:
            return None
        if not isinstance(issue, dict) or "pull_request" in issue:
            return None
        return issue

    # Tracker port -------------------------------------------------------

    def _event_notice(self, event: Any, cursor: str) -> MarkedNotice | None:
        if not isinstance(event, dict):
            return None
        event_id = _positive_int(event.get("id"))
        issue = event.get("issue")
        kind = event.get("event")
        if event_id is None or not isinstance(issue, dict) or "pull_request" in issue:
            return None
        number = _positive_int(issue.get("number"))
        if number is None:
            return None
        if kind in {"labeled", "unlabeled"}:
            raw = event.get("label")
            if not isinstance(raw, dict) or raw.get("name") != self.label:
                return None
        elif kind != "closed":
            return None
        sender = human_actor(event)
        if sender is None:
            return None
        return MarkedNotice(
            issue=self.issue(number),
            marker=self.label,
            actor=Actor(str(sender[0]), sender[1]),
            event_id=str(event_id),
            disposition=Disposition.ADMIT if kind == "labeled" else Disposition.CANCEL,
            cursor=cursor,
            occurred_at=parse_time(event.get("created_at")),
        )

    def _mention_notice(
        self, comment: Any, cursor: str, app_slug: str | None, answering: Collection[int]
    ) -> MarkedNotice | None:
        """A mention, or on an ``answering`` issue number any comment by a
        person as an answer (#5789 decision 8): one comment, one notice."""

        if not isinstance(comment, dict) or comment.get("performed_via_github_app") is not None:
            return None
        comment_id = _positive_int(comment.get("id"))
        body = comment.get("body")
        html_url = comment.get("html_url")
        issue_url = comment.get("issue_url")
        if (
            comment_id is None
            or not isinstance(body, str)
            or not body.strip()
            or not isinstance(issue_url, str)
            # A pull request conversation comment is review feedback, not a marking.
            or (isinstance(html_url, str) and "/pull/" in html_url)
        ):
            return None
        tail = issue_url.rstrip("/").rsplit("/", 1)[-1]
        number = int(tail) if tail.isdigit() and int(tail) > 0 else None
        sender = human_actor({"user": comment.get("user")})
        if number is None or sender is None:
            return None
        answer = number in answering
        if not answer and not mentions_factory(body, self.mention, app_slug):
            return None
        return MarkedNotice(
            issue=self.issue(number),
            marker=self.label if answer else f"@{self.mention}",
            actor=Actor(str(sender[0]), sender[1]),
            event_id=str(comment_id),
            disposition=Disposition.ANSWER if answer else Disposition.MENTION,
            cursor=cursor,
            occurred_at=parse_time(comment.get("created_at")),
        )

    async def _removal(self, issue: TrackerIssueRef, cursor: str) -> MarkedNotice | None:
        """The cancellation a running issue owes when its label is gone or it closed."""

        number = self._number(issue)
        current = await self._issue(number)
        if current is None:
            return None
        names = label_names(current)
        if names is None or current.get("state") not in {"open", "closed"}:
            raise Unavailable(f"{self._repo_path}/issues/{number}")
        if current.get("state") == "closed":
            kind, label = "closed", None
        elif self.label not in names:
            kind, label = "unlabeled", self.label
        else:
            return None
        events = await self._pages(f"{self._repo_path}/issues/{number}/events", {})
        event = last_event_of_kind(events, kind, label=label)
        if event is None:
            return None
        return self._event_notice({**event, "issue": {"number": number}}, cursor)

    async def poll_marked(
        self,
        cursor: str | None,
        *,
        running: Sequence[TrackerIssueRef],
        answering: Sequence[TrackerIssueRef],
    ) -> NoticePage:
        events_after, comments_after = _parse_cursor(cursor)
        answers = {
            int(issue.issue_id)
            for issue in answering
            if issue.kind == GITHUB
            and issue.host == self.host
            and issue.scope_id == str(self.repository_id)
            and issue.issue_id.isdigit()
        }
        # Read every listing before returning any notice or cursor.
        events = await self._pages(f"{self._repo_path}/issues/events", {})
        comments = await self._pages(
            f"{self._repo_path}/issues/comments",
            {"sort": "created", "direction": "desc"},
            newer_than=comments_after,
        )
        notices: list[MarkedNotice] = []
        app_slug = await self.app_slug()
        newest_event = events_after
        fresh = [
            event
            for event in events
            if isinstance(event, dict)
            and (event_id := _positive_int(event.get("id"))) is not None
            and event_id > events_after
        ]
        for event in sorted(fresh, key=lambda item: int(item["id"])):
            newest_event = max(newest_event, int(event["id"]))
            notice = self._event_notice(event, f"{event['id']}:{comments_after}")
            if notice is not None:
                notices.append(notice)
        newest_comment = comments_after
        for comment in reversed(comments):
            comment_id = _positive_int(comment.get("id")) if isinstance(comment, dict) else None
            if comment_id is None:
                continue
            newest_comment = max(newest_comment, comment_id)
            notice = self._mention_notice(
                comment, f"{newest_event}:{comment_id}", app_slug, answers
            )
            if notice is not None:
                notices.append(notice)
        next_cursor = f"{newest_event}:{newest_comment}"
        seen = {(notice.issue, notice.event_id) for notice in notices}
        for issue in running:
            removal = await self._removal(issue, next_cursor)
            if removal is not None and (removal.issue, removal.event_id) not in seen:
                notices.append(removal)
        return NoticePage(tuple(notices), next_cursor)

    async def verify_current(self, notice: MarkedNotice) -> bool:
        number = self._number(notice.issue)
        issue = await self._issue(number)
        if issue is None:
            return notice.disposition is Disposition.CANCEL
        names = label_names(issue) or set()
        state = issue.get("state")
        if notice.disposition is Disposition.CANCEL:
            if state not in {"open", "closed"} or label_names(issue) is None:
                raise Unavailable("issue_state")
            return state == "closed" or self.label not in names
        if state != "open":
            return False
        if notice.disposition is Disposition.ANSWER and self.label not in names:
            return False
        if notice.disposition is Disposition.ADMIT:
            if self.label not in names:
                return False
            events = await self._pages(f"{self._repo_path}/issues/{number}/events", {})
            latest = last_label_event(events, self.label)
            return latest is not None and str(latest.get("id")) == notice.event_id
        try:
            comment = await self._get(f"{self._repo_path}/issues/comments/{notice.event_id}")
        except NotFound:
            return False
        user = comment.get("user") if isinstance(comment, dict) else None
        body = comment.get("body") if isinstance(comment, dict) else None
        return (
            isinstance(comment, dict)
            and comment.get("issue_url") == f"{self._api}{self._repo_path}/issues/{number}"
            and comment.get("performed_via_github_app") is None
            and isinstance(user, dict)
            and str(user.get("id")) == notice.actor.id
            and isinstance(body, str)
            and (
                notice.disposition is Disposition.ANSWER
                or mentions_factory(body, self.mention, await self.app_slug())
            )
        )

    async def marking_actor(self, issue: TrackerIssueRef, marker: str) -> Actor | None:
        number = self._number(issue)
        current = await self._issue(number)
        if current is None or marker not in (label_names(current) or set()):
            return None
        events = await self._pages(f"{self._repo_path}/issues/{number}/events", {})
        event = last_label_event(events, marker)
        sender = human_actor(event) if event is not None else None
        return Actor(str(sender[0]), sender[1]) if sender is not None else None

    async def may_start(self, issue: TrackerIssueRef, actor: Actor) -> bool:
        """Current write or admin on the repository, for the actor's immutable id."""

        self._number(issue)
        permission = await self._get(
            f"{self._repo_path}/collaborators/{quote(actor.login, safe='')}/permission"
        )
        if not isinstance(permission, dict):
            raise Unavailable("permission")
        user = permission.get("user")
        if not isinstance(user, dict) or str(user.get("id")) != actor.id:
            return False
        return permission.get("permission") in ("write", "admin")

    async def read_ticket(self, issue: TrackerIssueRef) -> str:
        content = await self.read_issue_content(self._number(issue))
        parts = [f"# {content.title}", content.body]
        if content.earlier_comments_omitted:
            parts.append("_Earlier comments were not read._")
        for comment in content.comments:
            author = comment.author or "unknown"
            when = f" at {comment.created_at}" if comment.created_at else ""
            parts.append(f"## Comment by {author}{when}\n\n{comment.body}")
        return "\n\n".join(parts) + "\n"

    async def read_source_content(self, issue: TrackerIssueRef) -> SourceContent:
        """The issue's title and body from one issue read, never its comments.

        https://docs.github.com/en/rest/issues/issues#get-an-issue
        An empty body reads back as null and is the empty text.
        """

        found = await self._issue(self._number(issue))
        if found is None:
            raise NotFound("issue")
        title = found.get("title")
        if not isinstance(title, str):
            raise Unavailable("issue")
        return SourceContent(title=title, body=_text(found.get("body")))

    async def ticket_labels(self, issue: TrackerIssueRef) -> TicketLabels:
        """The issue's labels from one issue read; GitHub has no components."""

        number = self._number(issue)
        found = await self._issue(number)
        if found is None:
            raise NotFound("issue")
        names = label_names(found)
        if names is None:
            raise Unavailable("labels")
        returned_number = found.get("number")
        if returned_number is not None and (
            type(returned_number) is not int or returned_number != number
        ):
            raise Unavailable("issue")
        state = found.get("state")
        return TicketLabels(
            labels=frozenset(names),
            verified_open_issue=(
                state == "open"
                if returned_number == number and state in ("open", "closed")
                else None
            ),
        )

    async def set_state_label(
        self, issue: TrackerIssueRef, *, add: str | None, remove: Collection[str]
    ) -> None:
        """Add ``add`` and delete each of ``remove``; `Unavailable` when incomplete.

        Every label page is read before anything changes, so only a missing
        ``add`` is posted and only a present ``remove`` is deleted. A refused
        write is logged and given up, so it counts as done. Any other failure
        leaves the labels for the next pass.
        """

        number = self._number(issue)
        labels_path = f"{self._api}{self._repo_path}/issues/{number}/labels"
        template = "/repos/{owner}/{repo}/issues/{issue_number}/labels"
        headers = github_headers(await self._token())
        present = await self._present_labels(labels_path, template, headers)
        if add and add not in present:
            try:
                added = await github_request(
                    self._client,
                    "POST",
                    labels_path,
                    path_template=template,
                    headers=headers,
                    json={"labels": [add]},
                    follow_redirects=False,
                )
            except httpx.HTTPError:
                raise Unavailable("labels") from None
            if added.status_code in REFUSED_STATUSES:
                logger.info(
                    "factory state label refused",
                    extra={"issue_number": number, "status": added.status_code},
                )
            elif added.status_code not in {200, 201}:
                raise Unavailable("labels")
        complete = True
        for name in remove:
            if name not in present:
                continue
            try:
                removed = await github_request(
                    self._client,
                    "DELETE",
                    f"{labels_path}/{quote(name, safe=':')}",
                    path_template=template + "/{label}",
                    headers=headers,
                    follow_redirects=False,
                )
            except httpx.HTTPError:
                complete = False
                continue
            # 404: the label was not on the issue, which is the goal.
            if removed.status_code in {401, 403}:
                logger.info(
                    "factory state label removal refused",
                    extra={"issue_number": number, "status": removed.status_code},
                )
            elif removed.status_code not in {200, 204, 404}:
                complete = False
        if not complete:
            raise Unavailable("labels")

    async def _present_labels(
        self, labels_path: str, template: str, headers: dict[str, str]
    ) -> set[str]:
        """Every label name on the issue, following pagination on the same resource.

        https://docs.github.com/en/rest/issues/labels#list-labels-for-an-issue
        """

        resource = httpx.URL(labels_path)
        url = labels_path
        params: dict[str, int] | None = {"per_page": 100}
        present: set[str] = set()
        seen: set[str] = set()
        while True:
            try:
                listed = await github_request(
                    self._client,
                    "GET",
                    url,
                    path_template=template,
                    headers=headers,
                    params=params,
                    follow_redirects=False,
                )
                if listed.status_code != 200:
                    raise Unavailable("labels")
                payload = listed.json()
            except (httpx.HTTPError, ValueError):
                raise Unavailable("labels") from None
            if not isinstance(payload, list) or any(
                not isinstance(label, dict) or not isinstance(label.get("name"), str)
                for label in payload
            ):
                raise Unavailable("labels")
            present.update(label["name"] for label in payload)
            seen.add(str(listed.request.url))
            next_link = listed.links.get("next")
            if next_link is None:
                return present
            next_url = next_link.get("url")
            if not isinstance(next_url, str):
                raise Unavailable("labels")
            try:
                next_resource = httpx.URL(next_url)
            except httpx.InvalidURL:
                raise Unavailable("labels") from None
            if (
                next_resource.scheme,
                next_resource.host,
                next_resource.port,
                next_resource.path,
            ) != (resource.scheme, resource.host, resource.port, resource.path) or str(
                next_resource
            ) in seen:
                raise Unavailable("labels")
            url = str(next_resource)
            params = None

    def issue_url(self, issue: TrackerIssueRef) -> str:
        self._number(issue)
        return issue_url(self._html_base, self.repo_full_name, issue)

    async def closing_reference(self, issue: TrackerIssueRef, repository: RepositoryRef) -> str:
        number = self._number(issue)
        if repository.kind == GITHUB and repository.project_id == issue.scope_id:
            return f"Closes #{number}"
        return f"Closes {self.repo_full_name}#{number}"

    async def publication_references(
        self, issue: TrackerIssueRef, repository: RepositoryRef
    ) -> tuple[str, ...]:
        """The closing reference, which is already scope-qualified: a bare
        ``Closes #n`` only in the issue's own repository, else ``owner/repo#n``."""

        return (await self.closing_reference(issue, repository),)

    async def link_pull_request(self, issue: TrackerIssueRef, pull_request: PullRequest) -> None:
        self._number(issue)

    async def dependencies(self, issue: TrackerIssueRef) -> tuple[TrackerIssueRef, ...]:
        self._number(issue)
        return ()

    async def in_group(self, actor: Actor, group_id: str) -> bool:
        raise Unsupported(Operation.GROUP_MEMBERSHIP)

    # Ticket read --------------------------------------------------------

    async def _stream_json(
        self, url: str, *, token: str, params: dict[str, Any] | None = None
    ) -> tuple[Any, httpx.Headers]:
        async with self._client.stream(
            "GET",
            url,
            params=params,
            headers=github_headers(token),
            timeout=_ISSUE_READ_PROVIDER_TIMEOUT_SECONDS,
            follow_redirects=False,
        ) as response:
            if response.status_code != 200:
                raise Unavailable("issue")
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > _ISSUE_READ_MAX_RESPONSE_BYTES:
                    raise Unavailable("issue")
            return json.loads(body), response.headers

    async def _comment_page(
        self, base: str, token: str, page: int
    ) -> tuple[list[Any], httpx.Headers]:
        batch, headers = await self._stream_json(
            f"{base}/comments",
            token=token,
            params={"per_page": _ISSUE_READ_COMMENTS_PER_PAGE, "page": page},
        )
        if not isinstance(batch, list):
            raise Unavailable("issue comments")
        return batch, headers

    async def read_issue_content(self, number: int) -> IssueContent:
        """The issue and its comments, verbatim, addressed by repository id.

        Addressing the repository by its numeric id keeps a rename or a transfer
        from redirecting the read to a different repository than the WorkItem's.
        Nothing is parsed into the platform or stored.

        A thread of more than a thousand comments returns the newest thousand,
        oldest first. That costs at most 12 comment page requests plus the issue
        (page 1, the ten pages ending at the last page, and one earlier page
        when the last page is partial). A listing with `rel="next"` but no
        `rel="last"` is followed forward for at most ten pages, else refused.
        """

        base = f"{self._api}/repositories/{self.repository_id}/issues/{number}"
        token = await self._token()
        try:
            issue, _ = await self._stream_json(base, token=token)
            comments: list[IssueComment] = []
            first_batch, headers = await self._comment_page(base, token, 1)
            last_page = _last_page(headers)
            pages: dict[int, list[Any]] = {1: first_batch}
            if last_page is None:
                # No `rel="last"`: follow `rel="next"` for at most ten pages. A
                # longer listing cannot have its newest comments located.
                last_page = 1
                while 'rel="next"' in headers.get("link", ""):
                    if last_page == _ISSUE_READ_MAX_COMMENT_PAGES:
                        raise Unavailable("issue comments")
                    last_page += 1
                    pages[last_page], headers = await self._comment_page(base, token, last_page)
            first = max(1, last_page - _ISSUE_READ_MAX_COMMENT_PAGES + 1)
            for page in range(first, last_page + 1):
                if page not in pages:
                    pages[page] = (await self._comment_page(base, token, page))[0]
            if (
                last_page > _ISSUE_READ_MAX_COMMENT_PAGES
                and len(pages[last_page]) < _ISSUE_READ_COMMENTS_PER_PAGE
            ):
                # A partial last page leaves the newest ten pages short of a
                # thousand comments, so one earlier page completes the window.
                first -= 1
                if first not in pages:
                    pages[first] = (await self._comment_page(base, token, first))[0]
            window = [c for page in range(first, last_page + 1) for c in pages[page]]
            limit = _ISSUE_READ_MAX_COMMENT_PAGES * _ISSUE_READ_COMMENTS_PER_PAGE
            earlier_omitted = last_page > _ISSUE_READ_MAX_COMMENT_PAGES
            if earlier_omitted:
                window = window[-limit:]
            for comment in window:
                if not isinstance(comment, dict):
                    raise Unavailable("issue comments")
                created = comment.get("created_at")
                comments.append(
                    IssueComment(
                        author=_login(comment.get("user")),
                        created_at=created if isinstance(created, str) else None,
                        body=_text(comment.get("body")),
                    )
                )
        except (httpx.HTTPError, ValueError):
            raise Unavailable("issue") from None
        if (
            not isinstance(issue, dict)
            or type(issue.get("number")) is not int
            or issue["number"] != number
            or "pull_request" in issue
            or not isinstance(issue.get("title"), str)
        ):
            raise Unavailable("issue")
        state = issue.get("state")
        return IssueContent(
            title=issue["title"],
            body=_text(issue.get("body")),
            state=state if isinstance(state, str) else None,
            author=_login(issue.get("user")),
            comments=comments,
            earlier_comments_omitted=earlier_omitted,
        )

    # GitHub intake reads ------------------------------------------------

    async def verify_notice(self, notice: FactoryNotice) -> IssueFacts:
        """Confirm a webhook or polled notice against GitHub now.

        Raises `FactoryRefused` or `FeedbackUnavailable` with the codes the
        delivery audit records, exactly as the intake always has.
        """

        repository = await get_github_json(
            self._client,
            api=self._api,
            token=await self._token(),
            path=self._repo_path,
            refusal="repository_unavailable",
        )
        return await self.verify_issue(notice, repository)

    async def verify_issue(self, notice: FactoryNotice, repository: Any) -> IssueFacts:
        """`verify_notice` against a repository object already read.

        A poll pass reads its repository once (#4361 decision 14) and confirms
        each notice against that read; the issue is always re-read here,
        since its labels decide admission and cancellation.
        """

        token = await self._token()
        if not repository_identity_matches(
            repository,
            repository_id=notice.repository_id,
            repo_full_name=notice.repo_full_name,
        ):
            raise FactoryRefused("repository_mismatch")
        issue = await get_github_json(
            self._client,
            api=self._api,
            token=token,
            path=f"{self._repo_path}/issues/{notice.issue_number}",
            refusal="issue_unavailable",
        )
        if type(issue.get("number")) is not int or issue["number"] != notice.issue_number:
            raise FactoryRefused("issue_mismatch")
        if "pull_request" in issue:
            raise FactoryRefused("pull_request_issue")
        names = label_names(issue)
        if names is None:
            raise FactoryRefused("invalid_issue")
        # A ``base:`` label change is only recorded against an existing WorkItem;
        # the labels are what matter, so none of the per-disposition checks apply.
        if notice.disposition != "base_label":
            if notice.disposition == "admit":
                if issue.get("state") != "open" or notice.label not in names:
                    raise FactoryRefused(
                        "issue_not_open" if issue.get("state") != "open" else "label_absent"
                    )
            elif notice.action == "closed":
                if issue.get("state") != "closed":
                    raise FactoryRefused("issue_still_open")
            elif notice.action == "unlabeled":
                if notice.label in names:
                    raise FactoryRefused("label_still_present")
            else:
                if issue.get("state") != "open":
                    raise FactoryRefused("issue_not_open")
                await self._verify_mention(notice, token)
        if notice.disposition != "admit":
            await verify_sender_write_permission(
                self._client,
                api=self._api,
                token=token,
                repo_path=self._repo_path,
                sender_id=notice.sender_id,
                sender_login=notice.sender_login,
            )
        default_branch = repository.get("default_branch")
        return IssueFacts(
            labels=names,
            default_branch=default_branch if isinstance(default_branch, str) else None,
        )

    async def label_permission(self, notice: FactoryNotice) -> str | None:
        """Authorize a canonical actor after duplicate and delegated-output checks."""
        try:
            await verify_sender_write_permission(
                self._client,
                api=self._api,
                token=await self._token(),
                repo_path=self._repo_path,
                sender_id=notice.sender_id,
                sender_login=notice.sender_login,
            )
        except FeedbackIgnored as exc:
            if exc.code == "sender_permission_refused":
                return "actor_no_permission"
            raise FeedbackUnavailable(exc.code) from None
        return None

    async def _verify_mention(self, notice: FactoryNotice, token: str) -> None:
        comment = await get_github_json(
            self._client,
            api=self._api,
            token=token,
            path=f"{self._repo_path}/issues/comments/{notice.comment_id}",
            refusal="comment_unavailable",
        )
        if comment.get("issue_url") != f"{self._api}{self._repo_path}/issues/{notice.issue_number}":
            raise FactoryRefused("comment_target_mismatch")
        if comment.get("performed_via_github_app") is not None:
            raise FactoryRefused("app_authored")
        if comment.get("body") != notice.comment_body:
            raise FactoryRefused("comment_changed")
        user = comment.get("user")
        if (
            not isinstance(user, dict)
            or type(user.get("id")) is not int
            or user["id"] != notice.sender_id
        ):
            raise FactoryRefused("sender_mismatch")
        body = comment.get("body")
        if not isinstance(body, str) or not mentions_factory(
            body, self.mention, await self.app_slug()
        ):
            raise FactoryRefused("ordinary_comment")

    async def issue_state(self, number: int) -> IssueState:
        """The issue's state and labels now, and whether it is a pull request (#5380).

        https://docs.github.com/en/rest/issues/issues#get-an-issue: every pull
        request is also an issue, and its issue payload carries a
        ``pull_request`` key. A 404 is `NotFound`, which no retry changes; any
        other failure, or a payload that does not name this issue or lists
        malformed labels, is `Unavailable`.
        """

        issue = await self._get(f"{self._repo_path}/issues/{number}")
        if (
            not isinstance(issue, dict)
            or type(issue.get("number")) is not int
            or issue["number"] != number
        ):
            raise Unavailable("issue")
        names = label_names(issue)
        if names is None:
            raise Unavailable("issue")
        state = issue.get("state")
        return IssueState(
            state=state if isinstance(state, str) else None,
            labels=frozenset(names),
            pull_request="pull_request" in issue,
        )

    async def issue_events(self, number: int) -> list[Any]:
        """Every event on one issue, in GitHub's order, or `Unavailable`."""

        try:
            return await get_all(
                self._client,
                api=self._api,
                token=await self._token(),
                path=f"{self._repo_path}/issues/{number}/events",
                params={},
            )
        except TransportUnavailable:
            raise Unavailable("issue events") from None

    async def labeled_open_issues(self, etag: str | None) -> tuple[list[Any] | None, str | None]:
        """Open issues carrying the factory label; None when unchanged since ``etag``."""

        return await self._conditional_list(
            f"{self._repo_path}/issues", {"state": "open", "labels": self.label}, etag
        )

    async def issue_comments_since(
        self, since: str, etag: str | None
    ) -> tuple[list[Any] | None, str | None]:
        """Issue comments created or updated since ``since``, oldest first."""

        return await self._conditional_list(
            f"{self._repo_path}/issues/comments",
            {"since": since, "sort": "created", "direction": "asc"},
            etag,
        )

    async def _conditional_list(
        self, path: str, params: dict[str, Any], etag: str | None
    ) -> tuple[list[Any] | None, str | None]:
        try:
            return await list_pages(
                self._client,
                api=self._api,
                token=await self._token(),
                path=path,
                params=params,
                etag=etag,
            )
        except PollUnavailable:
            raise Unavailable(path) from None

    async def conditional_issue(
        self, number: int, etag: str | None
    ) -> tuple[dict[str, Any] | None, str | None]:
        """Read current state, without charging unchanged issues to the rate budget.

        Authenticated conditional requests returning 304 do not count against the
        primary rate limit. A cached value is used only for an open labeled issue.
        https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api#use-conditional-requests-if-appropriate
        """

        path = f"{self._repo_path}/issues/{number}"
        headers = github_headers(await self._token())
        if etag:
            headers["If-None-Match"] = etag
        try:
            response = await self._client.get(
                f"{self._api}{path}", headers=headers, follow_redirects=False
            )
        except httpx.HTTPError:
            raise Unavailable(path) from None
        if response.status_code == 304 and etag:
            return None, response.headers.get("etag") or etag
        if response.status_code != 200:
            raise Unavailable(path)
        try:
            issue = response.json()
        except ValueError:
            raise Unavailable(path) from None
        if not isinstance(issue, dict):
            raise Unavailable(path)
        return issue, response.headers.get("etag")


def _login(user: object) -> str | None:
    login = user.get("login") if isinstance(user, dict) else None
    return login if isinstance(login, str) else None


def _text(value: object) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise Unavailable("issue")
    return value
