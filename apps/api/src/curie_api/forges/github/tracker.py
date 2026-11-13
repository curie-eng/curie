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
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlsplit

import httpx

from curie_api.forges.capabilities import TRACKER_OPERATIONS, Operation, Support
from curie_api.forges.errors import Ambiguous, NotFound, Unauthorized, Unavailable, Unsupported
from curie_api.forges.github.comments import GitHubMarkedComments, TokenSource
from curie_api.forges.github.marked_comments import REFUSED_STATUSES, github_request
from curie_api.forges.github.review_polling import human_actor
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
from curie_api.forges.types import (
    GITHUB,
    Actor,
    Disposition,
    LinkedIssue,
    MarkedNotice,
    NoticePage,
    PullRequest,
    RepositoryRef,
    TicketLabels,
    TrackerIssueRef,
    body_sha256,
    ends_with_marker,
    linked_issue_body,
    linked_issue_marker,
    publication_url,
    valid_output_identity,
)
from curie_api.github_factory_events import FactoryNotice, FactoryRefused, mentions_login
from curie_api.github_review_truth import verify_sender_write_permission
from curie_api.repo_full_name import repo_url_path

# State label refusals have always been logged under the status comment pass.
logger = logging.getLogger("curie_api.factory_notices")

_ISSUE_READ_PROVIDER_TIMEOUT_SECONDS = 5.0
_ISSUE_READ_MAX_RESPONSE_BYTES = 2_097_152
_ISSUE_READ_COMMENTS_PER_PAGE = 100
# Ten pages is a thousand comments. A longer thread is truncated and says so,
# rather than turning one tool call into an unbounded walk of the API.
_ISSUE_READ_MAX_COMMENT_PAGES = 10
# A port listing longer than this many pages is not trusted to be complete.
_MAX_PAGES = 50


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
    comments_truncated: bool


@dataclass(frozen=True)
class IssueFacts:
    """What GitHub confirmed about a notice's issue and repository."""

    labels: set[str]
    default_branch: str | None


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
    if status == 404:
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


def linked_listing_params(own: str) -> dict[str, str]:
    """The query of the linked issue lookup: every issue ``own`` created, in
    any state, oldest first
    (https://docs.github.com/en/rest/issues/issues#list-repository-issues).
    ``per_page`` and ``page`` are added by the pager."""

    return {"creator": own, "state": "all", "sort": "created", "direction": "asc"}


def created_by(item: Mapping[str, Any], own: str) -> bool:
    """Whether the listed issue's author is ``own``, the App's bot user.

    The login is compared case-insensitively and the account type must be
    ``Bot``, so a person's issue never counts even if a listing ignored the
    ``creator`` filter.
    """

    user = item.get("user")
    if not isinstance(user, dict):
        return False
    login = user.get("login")
    return (
        isinstance(login, str) and login.casefold() == own.casefold() and user.get("type") == "Bot"
    )


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
        under the App JWT). ``create_linked_issue`` counts only issues that
        login created."""

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

    @property
    def marked_comments(self) -> GitHubMarkedComments:
        return self._comments

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

    def _mention_notice(self, comment: Any, cursor: str) -> MarkedNotice | None:
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
            or not mentions_login(body, self.mention)
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
        return MarkedNotice(
            issue=self.issue(number),
            marker=f"@{self.mention}",
            actor=Actor(str(sender[0]), sender[1]),
            event_id=str(comment_id),
            disposition=Disposition.MENTION,
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
        self, cursor: str | None, *, running: Sequence[TrackerIssueRef]
    ) -> NoticePage:
        events_after, comments_after = _parse_cursor(cursor)
        # Read every listing before returning any notice or cursor.
        events = await self._pages(f"{self._repo_path}/issues/events", {})
        comments = await self._pages(
            f"{self._repo_path}/issues/comments",
            {"sort": "created", "direction": "desc"},
            newer_than=comments_after,
        )
        notices: list[MarkedNotice] = []
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
            notice = self._mention_notice(comment, f"{newest_event}:{comment_id}")
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
            return False
        names = label_names(issue) or set()
        state = issue.get("state")
        if notice.disposition is Disposition.CANCEL:
            return state == "closed" or self.label not in names
        if state != "open":
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
            and mentions_login(body, self.mention)
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
        for comment in content.comments:
            author = comment.author or "unknown"
            when = f" at {comment.created_at}" if comment.created_at else ""
            parts.append(f"## Comment by {author}{when}\n\n{comment.body}")
        if content.comments_truncated:
            parts.append("_Later comments were not read._")
        return "\n\n".join(parts) + "\n"

    async def ticket_labels(self, issue: TrackerIssueRef) -> TicketLabels:
        """The issue's labels from one issue read; GitHub has no components."""

        found = await self._issue(self._number(issue))
        if found is None:
            raise NotFound("issue")
        names = label_names(found)
        if names is None:
            raise Unavailable("labels")
        return TicketLabels(labels=frozenset(names))

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

    # Linked output issues (ADR 0277) ------------------------------------

    @staticmethod
    def embed_linked_marker(output_identity: str) -> str:
        """How GitHub carries the linked issue marker: a hidden HTML comment."""

        return f"<!-- {linked_issue_marker(output_identity)} -->"

    async def _linked_matches(self, own: str, embedded: str) -> list[int]:
        """Numbers of the repository's issues ``own`` created whose last
        non-empty body line is ``embedded``, oldest first.

        Lists repository issues filtered by creator, never the search API,
        whose index lags a create
        (https://docs.github.com/en/rest/issues/issues#list-repository-issues).
        That listing returns pull requests too, marked by a ``pull_request``
        key; they are skipped. The creator filter is the server's; the owner
        check (`created_by`) is ours. A listing past the page bound, an own
        item whose body is not text, or a matching item without a usable
        number is `Unavailable`: absence is proven only by a readable listing.
        """

        items = await self._pages(f"{self._repo_path}/issues", linked_listing_params(own))
        matches: list[int] = []
        for item in items:
            if (
                not isinstance(item, dict)
                or "pull_request" in item
                or not created_by(item, own)
                or not ends_with_marker(_text(item.get("body")), embedded)
            ):
                continue
            number = _positive_int(item.get("number"))
            if number is None:
                raise Unavailable("linked_issue")
            matches.append(number)
        return matches

    async def _create_issue(self, title: str, body: str, labels: Collection[str]) -> int | None:
        """POST the issue; its number, or None when the answer is uncertain.

        https://docs.github.com/en/rest/issues/issues#create-an-issue
        A transport error, a 5xx or an unreadable 201 may have created it, so
        it is None and the caller lists again. A definite refusal raises:
        401 or 403 `Unauthorized`, 404 `NotFound`, 422 and anything else
        `Unavailable`.
        """

        template = "/repos/{owner}/{repo}/issues"
        try:
            response = await github_request(
                self._client,
                "POST",
                f"{self._api}{self._repo_path}/issues",
                path_template=template,
                headers=github_headers(await self._token()),
                json={"title": title, "body": body, "labels": sorted(set(labels))},
                follow_redirects=False,
            )
        except httpx.HTTPError:
            return None
        status = response.status_code
        if status >= 500:
            return None
        if status in {401, 403}:
            raise Unauthorized("linked_issue")
        if status == 404:
            raise NotFound("linked_issue")
        if status == 422:
            raise Unavailable("unprocessable")
        if status != 201:
            raise Unavailable("linked_issue")
        try:
            payload = response.json()
        except ValueError:
            return None
        return _positive_int(payload.get("number")) if isinstance(payload, dict) else None

    async def _single(self, own: str, embedded: str) -> int | None:
        matches = await self._linked_matches(own, embedded)
        if len(matches) > 1:
            raise Ambiguous("linked_issue")
        return matches[0] if matches else None

    async def create_linked_issue(
        self,
        source: TrackerIssueRef,
        *,
        output_identity: str,
        title: str,
        body: str,
        labels: Collection[str],
        expected: TrackerIssueRef | None = None,
    ) -> LinkedIssue:
        """One issue in this repository linked to ``source`` (ADR 0277).

        The body ends with ``Related to #<source>.``, a bare same-repository
        reference that makes GitHub's cross-reference and neither closes the
        source nor records a dependency, then `embed_linked_marker`. The
        lookup (`_linked_matches`) runs before the create and again after an
        uncertain create; one match reconciles, none after an uncertain
        create is `Unavailable("linked_issue_unconfirmed")`, two or more are
        `Ambiguous`. The listing can lag a create by seconds, so with
        ``expected`` nothing is listed or created: `_recorded_issue` reads that
        issue directly. The issue is then read back
        (https://docs.github.com/en/rest/issues/issues#get-an-issue) for its
        body digest, labels and ``html_url``.
        """

        valid_output_identity(output_identity)
        source_number = self._number(source)
        embedded = self.embed_linked_marker(output_identity)
        text = linked_issue_body(body, related=f"Related to #{source_number}.", embedded=embedded)
        own = await self._own_login()
        if expected is not None:
            return await self._recorded_issue(expected, own, embedded, text)
        number = await self._single(own, embedded)
        created = False
        if number is None:
            number = await self._create_issue(title, text, labels)
            created = number is not None
            if number is None:
                number = await self._single(own, embedded)
                if number is None:
                    raise Unavailable("linked_issue_unconfirmed")
        return await self._linked_issue(number, text, created=created)

    async def _recorded_issue(
        self, expected: TrackerIssueRef, own: str, embedded: str, sent: str
    ) -> LinkedIssue:
        """The recorded output issue read directly, with no listing
        (https://docs.github.com/en/rest/issues/issues#get-an-issue).

        ``NotFound("linked_issue")`` unless it is in this repository, is not a
        pull request, `created_by` ``own``, and its last non-empty line is
        ``embedded``.
        """

        try:
            number = self._number(expected)
        except NotFound:
            raise NotFound("linked_issue") from None
        found = await self._issue(number)
        if (
            found is None
            or not created_by(found, own)
            or not ends_with_marker(_text(found.get("body")), embedded)
        ):
            raise NotFound("linked_issue")
        return self._linked_result(number, found, sent, created=False)

    async def _linked_issue(self, number: int, sent: str, *, created: bool) -> LinkedIssue:
        found = await self._issue(number)
        if found is None:
            raise Unavailable("linked_issue")
        return self._linked_result(number, found, sent, created=created)

    def _linked_result(
        self, number: int, found: dict[str, Any], sent: str, *, created: bool
    ) -> LinkedIssue:
        # An empty body reads back as null; anything else that is not text
        # cannot be digested and is `Unavailable`.
        body = _text(found.get("body"))
        names = label_names(found)
        if names is None:
            raise Unavailable("linked_issue")
        ref = self.issue(number)
        try:
            url = publication_url(found.get("html_url"), self._html_base)
        except Unavailable:
            url = issue_url(self._html_base, self.repo_full_name, ref)
        return LinkedIssue(
            issue=ref,
            url=url,
            labels=frozenset(names),
            body_sha256=body_sha256(body),
            sent_body_sha256=body_sha256(sent),
            created=created,
        )

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

    async def read_issue_content(self, number: int) -> IssueContent:
        """The issue and its comments, verbatim, addressed by repository id.

        Addressing the repository by its numeric id keeps a rename or a transfer
        from redirecting the read to a different repository than the WorkItem's.
        Nothing is parsed into the platform or stored.
        """

        base = f"{self._api}/repositories/{self.repository_id}/issues/{number}"
        token = await self._token()
        try:
            issue, _ = await self._stream_json(base, token=token)
            comments: list[IssueComment] = []
            truncated = False
            for page in range(1, _ISSUE_READ_MAX_COMMENT_PAGES + 1):
                batch, headers = await self._stream_json(
                    f"{base}/comments",
                    token=token,
                    params={"per_page": _ISSUE_READ_COMMENTS_PER_PAGE, "page": page},
                )
                if not isinstance(batch, list):
                    raise Unavailable("issue comments")
                for comment in batch:
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
                if 'rel="next"' not in headers.get("link", ""):
                    break
            else:
                truncated = True
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
            comments_truncated=truncated,
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
        if not isinstance(body, str) or not mentions_login(body, self.mention):
            raise FactoryRefused("ordinary_comment")

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
