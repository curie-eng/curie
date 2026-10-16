"""A Jira Cloud REST v3 fake for the Jira tracker adapter (#4358 decision 25).

A Starlette app holding one Jira site's state: users and groups, issues with
labels, status, components and a changelog, comments with entity properties,
and remote links. The adapter reaches it over real HTTP, through
``httpx.ASGITransport`` in unit tests or uvicorn on 127.0.0.1 in the offline
lifecycle test, at either API form decision 2 names:

1. classic, ``https://<site>/rest/api/3/...``;
2. scoped, ``https://<gateway>/ex/jira/<cloudId>/rest/api/3/...``.

``/_edge/tenant_info`` answers only on the site host, as on Jira Cloud.

Responses are built from the templates in ``forge_fakes/jira_payloads/``, which
follow the response examples of Atlassian's REST v3 reference, anonymized. They
carry the fields the adapter drops (``self``, ``avatarUrls``, ``expand``,
``issuetype`` ...) so the adapter is proven to read only what it needs. Times
are rendered as Jira renders them, ``yyyy-MM-dd'T'HH:mm:ss.SSSZ`` in the
requesting user's time zone, so an adapter that assumes UTC is caught.

Things the fake enforces rather than records:

1. basic auth with the registered email and API token (401 otherwise), so a
   wrong token is a real 401;
2. the search JQL is exactly the decision 7 shape (400 otherwise), and its
   ``updated >= "yyyy/MM/dd HH:mm"`` is read in the caller's time zone with
   minute precision, as Jira reads it;
3. ``page_cap`` caps every listing page below the requested ``maxResults``, as
   Jira may, so a client that stops at the first page loses items;
4. history ids and comment ids are two separate sequences that both start at
   10100, as two Jira tables do, so the same number can name a label event and
   a comment on one site.

Everything the adapter sends is in ``requests``; every write it makes through
HTTP (not through seeding) counts one in ``writes``. Nothing here is a real
site, account, cloud id or token.
"""

from __future__ import annotations

import base64
import copy
import itertools
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from curie_api.forges import types
from curie_api.forges.config import ForgesConfig
from curie_api.forges.registry import ForgeRegistry, build_registry
from curie_api.forges.types import Actor, TrackerIssueRef
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Mount, Route

PAYLOADS = Path(__file__).resolve().parent / "jira_payloads"

# The site and its identity. Placeholders only.
SITE_HOST = "acme.atlassian.example"
SITE = f"https://{SITE_HOST}"
GATEWAY_HOST = "api.atlassian.example"
CLOUD_ID = "11111111-1111-1111-1111-111111111111"
SCOPED_API = f"https://{GATEWAY_HOST}/ex/jira/{CLOUD_ID}"

BOT_EMAIL = "factory-bot@example.com"
# A recognizable value so a leak is unmistakable in a log or a traceback.
TOKEN = "jira-fixture-token-must-never-be-logged-7f3a"
BOT_ACCOUNT_ID = "712020:00000000-0000-4000-8000-0000000000b0"
BOT_NAME = "Curie Factory"
BOT_TIME_ZONE = "America/New_York"

LABEL = "factory"
TRACKER_NAME = "acme-site"
BINDING_NAME = "acme-jira"
CODE_HOST_NAME = "github"
GITHUB_REPO = "acme-corp/acme-bot"
GITHUB_REPO_ID = "4401"
STARTERS = "starters"

# 2026-03-08 06:00:00 UTC: one hour before New York springs forward.
START_MS = int(datetime(2026, 3, 8, 6, 0, tzinfo=UTC).timestamp() * 1000)

# Listing reads a page fault can hit (the harness's `fail_next_page`).
LISTINGS = frozenset({"search", "changelog", "comments"})
# Requests that change site state.
WRITES = frozenset(
    {
        "comment_create",
        "comment_update",
        "comment_property_put",
        "issue_update",
        "remote_link_create",
    }
)
STATUSES = ("To Do", "In Progress", "Done")

_JQL = re.compile(
    r'labels = "(?P<label>[^"\\]+)" AND statusCategory != Done'
    r'(?: AND updated >= "(?P<since>\d{4}/\d{2}/\d{2} \d{2}:\d{2})")?'
    r" ORDER BY updated ASC"
)


def payload(name: str) -> Any:
    """A deep copy of one template in ``jira_payloads``."""

    return json.loads((PAYLOADS / f"{name}.json").read_text())


def jira_time(ms: int, zone: str) -> str:
    """``ms`` as Jira renders a date-time field: ``2026-03-08T01:59:30.000-0500``."""

    moment = datetime.fromtimestamp(ms / 1000, ZoneInfo(zone))
    return f"{moment.strftime('%Y-%m-%dT%H:%M:%S')}.{ms % 1000:03d}{moment.strftime('%z')}"


def ms_of(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def basic(username: str, token: str) -> str:
    return "Basic " + base64.b64encode(f"{username}:{token}".encode()).decode()


# ADF ---------------------------------------------------------------------------


def adf_text(text: str) -> dict[str, Any]:
    """A document of one paragraph per blank-line separated block."""

    blocks = [block for block in text.split("\n\n") if block]
    return {
        "type": "doc",
        "version": 1,
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": block}]} for block in blocks
        ],
    }


def adf_mention(account_id: str, display_name: str, *, before: str = "", after: str = "") -> dict:
    """A comment body whose one paragraph mentions ``account_id``."""

    inline: list[dict[str, Any]] = []
    if before:
        inline.append({"type": "text", "text": before})
    inline.append(
        {
            "type": "mention",
            "attrs": {"id": account_id, "text": f"@{display_name}", "accessLevel": ""},
        }
    )
    if after:
        inline.append({"type": "text", "text": after})
    return {"type": "doc", "version": 1, "content": [{"type": "paragraph", "content": inline}]}


def flatten(node: Any) -> str:
    """Every text node's text in a document, paragraphs on their own lines."""

    if isinstance(node, list):
        return "".join(flatten(child) for child in node)
    if not isinstance(node, Mapping):
        return ""
    if node.get("type") == "text":
        return str(node.get("text", ""))
    inner = flatten(node.get("content", []))
    return inner + "\n" if node.get("type") in {"paragraph", "heading", "codeBlock"} else inner


# State -------------------------------------------------------------------------


@dataclass
class JiraUser:
    account_id: str
    display_name: str
    account_type: str = "atlassian"
    time_zone: str = "UTC"
    email: str | None = None

    def bean(self) -> dict[str, Any]:
        bean = payload("user")
        bean.update(
            {
                "self": f"{SITE}/rest/api/3/user?accountId={self.account_id}",
                "accountId": self.account_id,
                "accountType": self.account_type,
                "displayName": self.display_name,
                "timeZone": self.time_zone,
            }
        )
        if self.email is None:
            # Email visibility is a profile setting; most beans omit it.
            bean.pop("emailAddress", None)
        else:
            bean["emailAddress"] = self.email
        return bean

    @property
    def actor(self) -> Actor:
        return Actor(id=self.account_id, login=self.display_name)


@dataclass
class JiraComment:
    id: int
    issue_id: int
    author: JiraUser
    body: dict[str, Any]
    created_ms: int
    updated_ms: int
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class JiraIssue:
    id: int
    key: str
    summary: str
    description: dict[str, Any] | None
    reporter: JiraUser
    created_ms: int
    updated_ms: int
    labels: list[str] = field(default_factory=list)
    components: list[str] = field(default_factory=list)
    status: str = "To Do"
    history: list[dict[str, Any]] = field(default_factory=list)
    comments: list[JiraComment] = field(default_factory=list)
    remote_links: list[dict[str, Any]] = field(default_factory=list)
    old_keys: list[str] = field(default_factory=list)
    indexed: bool = True


@dataclass
class Recorded:
    """One request the adapter made."""

    kind: str
    method: str
    host: str
    path: str
    params: dict[str, str]
    json: Any
    authorization: str | None
    status: int = 0


@dataclass
class _Fault:
    kinds: frozenset[str]
    countdown: int
    status: int
    headers: dict[str, str]
    body: Any


class JiraFake:
    """One Jira Cloud site. Seed it with the methods below, never its fields."""

    def __init__(
        self,
        *,
        cloud_id: str = CLOUD_ID,
        bot_account_id: str = BOT_ACCOUNT_ID,
        bot_type: str = "app",
        bot_time_zone: str = BOT_TIME_ZONE,
        page_cap: int | None = None,
        start_ms: int = START_MS,
        token: str = TOKEN,
    ) -> None:
        """``token`` is the API token the site accepts for the bot's email."""

        self.cloud_id = cloud_id
        self.page_cap = page_cap
        self.now_ms = start_ms
        self.users: dict[str, JiraUser] = {}
        self.groups: dict[str, set[str]] = {}
        self.issues: dict[int, JiraIssue] = {}
        self.requests: list[Recorded] = []
        self.writes = 0
        self.forbid_label_edits = False
        self.forbid_group_reads = False
        self.bot = self.add_user(
            bot_account_id,
            BOT_NAME,
            account_type=bot_type,
            time_zone=bot_time_zone,
            email=BOT_EMAIL,
        )
        self._credentials: dict[str, JiraUser] = {basic(BOT_EMAIL, token): self.bot}
        self.reporter = self.add_user("5b10a0000000000000000001", "Mia Krystof")
        self._issue_ids = itertools.count(10001)
        self._history_ids = itertools.count(10100)
        self._comment_ids = itertools.count(10100)
        self._link_ids = itertools.count(10900)
        self._keys: dict[str, itertools.count[int]] = {}
        self._faults: list[_Fault] = []
        self.app = self._build_app()

    # Clock ---------------------------------------------------------------

    def advance(self, seconds: float) -> int:
        self.now_ms += int(seconds * 1000)
        return self.now_ms

    def _tick(self) -> int:
        """The time of a change now; the clock then moves one second on."""

        stamp = self.now_ms
        self.now_ms += 1000
        return stamp

    # Users and groups ----------------------------------------------------

    def add_user(
        self,
        account_id: str,
        display_name: str,
        *,
        account_type: str = "atlassian",
        time_zone: str = "UTC",
        email: str | None = None,
    ) -> JiraUser:
        user = JiraUser(account_id, display_name, account_type, time_zone, email)
        self.users[account_id] = user
        return user

    def user_for(self, actor: Actor, *, account_type: str = "atlassian") -> JiraUser:
        known = self.users.get(actor.id)
        if known is not None:
            return known
        return self.add_user(actor.id, actor.login, account_type=account_type)

    def set_group_member(self, group_id: str, account_id: str, member: bool) -> None:
        members = self.groups.setdefault(group_id, set())
        (members.add if member else members.discard)(account_id)

    def accept_credential(self, username: str, token: str, user: JiraUser) -> None:
        self._credentials[basic(username, token)] = user

    # Issues --------------------------------------------------------------

    def seed_issue(
        self,
        summary: str,
        description: str | dict[str, Any] | None = None,
        *,
        project: str = "PROJ",
        components: Iterable[str] = (),
        status: str = "To Do",
        indexed: bool = True,
    ) -> JiraIssue:
        counter = self._keys.setdefault(project, itertools.count(1))
        created = self._tick()
        issue = JiraIssue(
            id=next(self._issue_ids),
            key=f"{project}-{next(counter)}",
            summary=summary,
            description=adf_text(description) if isinstance(description, str) else description,
            reporter=self.reporter,
            created_ms=created,
            updated_ms=created,
            components=list(components),
            status=status,
            indexed=indexed,
        )
        self.issues[issue.id] = issue
        return issue

    def ref(self, issue: JiraIssue, *, display_key: str | None = "current") -> TrackerIssueRef:
        """The TrackerIssueRef decision 3 gives ``issue`` on this site."""

        key = issue.key if display_key == "current" else display_key
        return TrackerIssueRef(types.JIRA_CLOUD, SITE_HOST, self.cloud_id, str(issue.id), key)

    def issue(self, ref: TrackerIssueRef | int | str) -> JiraIssue:
        issue_id = ref.issue_id if isinstance(ref, TrackerIssueRef) else ref
        return self.issues[int(issue_id)]

    def _change(
        self, issue: JiraIssue, by: JiraUser, items: list[dict[str, Any]], at: int | None
    ) -> str:
        stamp = self._tick() if at is None else at
        history_id = str(next(self._history_ids))
        issue.history.append({"id": history_id, "author": by, "created_ms": stamp, "items": items})
        issue.updated_ms = max(issue.updated_ms, stamp)
        return history_id

    def _labels_change(self, issue: JiraIssue, by: JiraUser, new: list[str], at: int | None) -> str:
        item = {
            "field": "labels",
            "fieldtype": "jira",
            "fieldId": "labels",
            "from": None,
            "fromString": " ".join(issue.labels),
            "to": None,
            "toString": " ".join(new),
        }
        issue.labels = new
        return self._change(issue, by, [item], at)

    def add_label(
        self, ref: TrackerIssueRef | int, label: str, by: JiraUser, *, at: int | None = None
    ) -> str:
        """``by`` adds ``label``; returns the changelog history id."""

        issue = self.issue(ref)
        return self._labels_change(issue, by, [*issue.labels, label], at)

    def remove_label(
        self, ref: TrackerIssueRef | int, label: str, by: JiraUser, *, at: int | None = None
    ) -> str:
        issue = self.issue(ref)
        return self._labels_change(issue, by, [name for name in issue.labels if name != label], at)

    def set_status(
        self, ref: TrackerIssueRef | int, status: str, by: JiraUser, *, at: int | None = None
    ) -> str:
        issue = self.issue(ref)
        statuses = payload("statuses")
        item = {
            "field": "status",
            "fieldtype": "jira",
            "fieldId": "status",
            "from": statuses[issue.status]["id"],
            "fromString": issue.status,
            "to": statuses[status]["id"],
            "toString": status,
        }
        issue.status = status
        return self._change(issue, by, [item], at)

    def edit_summary(self, ref: TrackerIssueRef | int, summary: str, by: JiraUser) -> str:
        issue = self.issue(ref)
        item = {
            "field": "summary",
            "fieldtype": "jira",
            "fieldId": "summary",
            "from": None,
            "fromString": issue.summary,
            "to": None,
            "toString": summary,
        }
        issue.summary = summary
        return self._change(issue, by, [item], None)

    def add_comment(
        self,
        ref: TrackerIssueRef | int,
        author: JiraUser,
        body: str | dict[str, Any],
        *,
        properties: Mapping[str, Any] | None = None,
        at: int | None = None,
    ) -> str:
        """``author`` comments; returns the comment id. A comment updates the issue."""

        issue = self.issue(ref)
        stamp = self._tick() if at is None else at
        comment = JiraComment(
            id=next(self._comment_ids),
            issue_id=issue.id,
            author=author,
            body=adf_text(body) if isinstance(body, str) else copy.deepcopy(body),
            created_ms=stamp,
            updated_ms=stamp,
            properties=dict(properties or {}),
        )
        issue.comments.append(comment)
        issue.updated_ms = max(issue.updated_ms, stamp)
        return str(comment.id)

    def move_issue(self, ref: TrackerIssueRef | int, new_key: str) -> None:
        """Move the issue to another project: a new key, the same id."""

        issue = self.issue(ref)
        issue.old_keys.append(issue.key)
        issue.key = new_key

    def move_issue_logged(self, ref: TrackerIssueRef | int, new_key: str, by: JiraUser) -> str:
        """Move the issue as Jira records a move: a new key, the same id, and one
        changelog history whose items name the old and new key and project.

        The ``Key`` item is how the changelog reports a key change
        (https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-issueidorkey-changelog-get,
        ``ChangeDetails``: ``field``, ``fieldtype``, ``from``, ``fromString``,
        ``to``, ``toString``). Returns the history id.
        """

        issue = self.issue(ref)
        old_key = issue.key
        items = [
            {
                "field": "project",
                "fieldtype": "jira",
                "fieldId": "project",
                "from": None,
                "fromString": old_key.split("-")[0],
                "to": None,
                "toString": new_key.split("-")[0],
            },
            {
                "field": "Key",
                "fieldtype": "jira",
                "from": None,
                "fromString": old_key,
                "to": None,
                "toString": new_key,
            },
        ]
        self.move_issue(issue.id, new_key)
        return self._change(issue, by, items, None)

    def delete_issue(self, ref: TrackerIssueRef | int) -> None:
        del self.issues[int(self.issue(ref).id)]

    def set_indexed(self, ref: TrackerIssueRef | int, indexed: bool) -> None:
        """Search sees the issue only once the index has caught up."""

        self.issue(ref).indexed = indexed

    def comment(self, comment_id: str | int) -> JiraComment:
        for issue in self.issues.values():
            for comment in issue.comments:
                if comment.id == int(comment_id):
                    return comment
        raise KeyError(comment_id)

    # Faults --------------------------------------------------------------

    def fail_next(
        self,
        kinds: str | Iterable[str],
        *,
        status: int = 500,
        after: int = 0,
        headers: Mapping[str, str] | None = None,
        body: Any = None,
    ) -> None:
        """The request of one of ``kinds`` made after ``after`` more such
        requests answers ``status`` once."""

        wanted = frozenset({kinds} if isinstance(kinds, str) else kinds)
        self._faults.append(_Fault(wanted, after, status, dict(headers or {}), body))

    def fail_next_page(self, *, after: int = 0) -> None:
        """The listing page (search, changelog or comments) read after ``after``
        more successful listing reads fails once with a 500."""

        self.fail_next(LISTINGS, status=500, after=after)

    def _fault_for(self, kind: str) -> _Fault | None:
        for fault in self._faults:
            if kind not in fault.kinds:
                continue
            if fault.countdown == 0:
                self._faults.remove(fault)
                return fault
            fault.countdown -= 1
            return None
        return None

    # Observations --------------------------------------------------------

    def requests_of(self, kind: str) -> list[Recorded]:
        return [request for request in self.requests if request.kind == kind]

    def own_comments(self, ref: TrackerIssueRef | int) -> list[JiraComment]:
        return [c for c in self.issue(ref).comments if c.author.account_id == self.bot.account_id]

    # Rendering -----------------------------------------------------------

    def _time(self, ms: int) -> str:
        return jira_time(ms, self._caller_zone)

    def _status_bean(self, name: str) -> dict[str, Any]:
        return payload("statuses")[name]

    def _issue_bean(self, issue: JiraIssue, fields: list[str] | None) -> dict[str, Any]:
        bean = payload("issue")
        bean.update(
            {"id": str(issue.id), "key": issue.key, "self": f"{SITE}/rest/api/3/issue/{issue.id}"}
        )
        values = bean["fields"]
        project = issue.key.split("-")[0]
        values.update(
            {
                "summary": issue.summary,
                "description": copy.deepcopy(issue.description),
                "labels": list(issue.labels),
                "status": self._status_bean(issue.status),
                "components": [
                    {
                        "self": f"{SITE}/rest/api/3/component/{10000 + index}",
                        "id": str(10000 + index),
                        "name": name,
                    }
                    for index, name in enumerate(issue.components)
                ],
                "updated": self._time(issue.updated_ms),
                "created": self._time(issue.created_ms),
                "reporter": issue.reporter.bean(),
                "creator": issue.reporter.bean(),
            }
        )
        values["project"].update({"key": project, "name": f"{project} project"})
        values["watches"]["self"] = f"{SITE}/rest/api/3/issue/{issue.key}/watchers"
        if fields is not None and not {"*all", "*navigable"} & set(fields):
            bean["fields"] = {name: values[name] for name in fields if name in values}
        return bean

    def _history_bean(self, entry: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "id": entry["id"],
            "author": entry["author"].bean(),
            "created": self._time(entry["created_ms"]),
            "items": copy.deepcopy(entry["items"]),
        }

    def _comment_bean(self, comment: JiraComment, *, properties: bool = False) -> dict[str, Any]:
        bean = payload("comment")
        bean.update(
            {
                "self": f"{SITE}/rest/api/3/issue/{comment.issue_id}/comment/{comment.id}",
                "id": str(comment.id),
                "author": comment.author.bean(),
                "updateAuthor": comment.author.bean(),
                "body": copy.deepcopy(comment.body),
                "created": self._time(comment.created_ms),
                "updated": self._time(comment.updated_ms),
            }
        )
        if properties:
            bean["properties"] = [
                {"key": key, "value": copy.deepcopy(value)}
                for key, value in comment.properties.items()
            ]
        return bean

    def _page_size(self, requested: int, maximum: int) -> int:
        size = max(1, min(requested, maximum))
        return min(size, self.page_cap) if self.page_cap else size

    # Transport -----------------------------------------------------------

    def _build_app(self) -> Starlette:
        api = [
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-myself/#api-rest-api-3-myself-get
            Route("/myself", self._endpoint("myself", self._myself), methods=["GET"]),
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-server-info/#api-rest-api-3-serverinfo-get
            Route("/serverInfo", self._endpoint("server_info", self._server_info)),
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/#api-rest-api-3-search-jql-post
            Route("/search/jql", self._endpoint("search", self._search), methods=["POST"]),
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-issueidorkey-changelog-get
            Route(
                "/issue/{issue}/changelog",
                self._endpoint("changelog", self._changelog),
                methods=["GET"],
            ),
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/#api-rest-api-3-issue-issueidorkey-comment-get
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/#api-rest-api-3-issue-issueidorkey-comment-post
            Route(
                "/issue/{issue}/comment",
                self._endpoint("comments", self._comments, post="comment_create"),
                methods=["GET", "POST"],
            ),
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/#api-rest-api-3-issue-issueidorkey-comment-id-put
            Route(
                "/issue/{issue}/comment/{comment}",
                self._endpoint("comment_update", self._comment_update),
                methods=["PUT"],
            ),
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-remote-links/#api-rest-api-3-issue-issueidorkey-remotelink-get
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-remote-links/#api-rest-api-3-issue-issueidorkey-remotelink-post
            Route(
                "/issue/{issue}/remotelink",
                self._endpoint("remote_links", self._remote_links, post="remote_link_create"),
                methods=["GET", "POST"],
            ),
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-issueidorkey-get
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-issueidorkey-put
            Route(
                "/issue/{issue}",
                self._endpoint("issue", self._issue, put="issue_update"),
                methods=["GET", "PUT"],
            ),
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/#api-rest-api-3-comment-list-post
            Route(
                "/comment/list",
                self._endpoint("comment_list", self._comment_list),
                methods=["POST"],
            ),
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comment-properties/#api-rest-api-3-comment-commentid-properties-propertykey-get
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comment-properties/#api-rest-api-3-comment-commentid-properties-propertykey-put
            Route(
                "/comment/{comment}/properties/{key}",
                self._endpoint(
                    "comment_property_get", self._comment_property, put="comment_property_put"
                ),
                methods=["GET", "PUT"],
            ),
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-users/#api-rest-api-3-user-groups-get
            Route("/user/groups", self._endpoint("user_groups", self._user_groups)),
        ]
        return Starlette(
            routes=[
                # https://support.atlassian.com/jira/kb/retrieve-my-atlassian-sites-cloud-id/
                Route(
                    "/_edge/tenant_info",
                    self._endpoint("tenant_info", self._tenant_info, anonymous=True),
                ),
                Mount("/ex/jira/{cloud_id}/rest/api/3", routes=api),
                Mount("/rest/api/3", routes=api),
            ]
        )

    def _endpoint(
        self,
        kind: str,
        handler: Any,
        *,
        post: str | None = None,
        put: str | None = None,
        anonymous: bool = False,
    ) -> Any:
        async def endpoint(request: Request) -> Response:
            effective = {"POST": post, "PUT": put}.get(request.method) or kind
            raw = await request.body()
            try:
                body = json.loads(raw) if raw else None
            except ValueError:
                body = None
            recorded = Recorded(
                kind=effective,
                method=request.method,
                host=request.url.hostname or "",
                path=request.url.path,
                params=dict(request.query_params),
                json=body,
                authorization=request.headers.get("authorization"),
            )
            self.requests.append(recorded)
            response = await self._answer(request, recorded, handler, anonymous)
            recorded.status = response.status_code
            if effective in WRITES and response.status_code < 300:
                self.writes += 1
            return response

        return endpoint

    async def _answer(
        self, request: Request, recorded: Recorded, handler: Any, anonymous: bool
    ) -> Response:
        cloud_id = request.path_params.get("cloud_id")
        if cloud_id is not None and cloud_id != self.cloud_id:
            return JSONResponse(payload("error_not_found"), status_code=404)
        caller = self._credentials.get(recorded.authorization or "")
        if caller is None and not anonymous:
            return JSONResponse(payload("error_unauthorized"), status_code=401)
        self._caller_zone = caller.time_zone if caller is not None else "UTC"
        fault = self._fault_for(recorded.kind)
        if fault is not None:
            body = fault.body if fault.body is not None else payload("error_rate_limited")
            return JSONResponse(body, status_code=fault.status, headers=fault.headers)
        return await handler(request, recorded, caller)

    def _find(self, id_or_key: str) -> JiraIssue | None:
        for issue in self.issues.values():
            if id_or_key in (str(issue.id), issue.key, *issue.old_keys):
                return issue
        return None

    @staticmethod
    def _not_found() -> Response:
        return JSONResponse(payload("error_not_found"), status_code=404)

    # Handlers ------------------------------------------------------------

    async def _tenant_info(self, request: Request, recorded: Recorded, caller: Any) -> Response:
        if recorded.host != SITE_HOST:
            return Response("Not Found", status_code=404, media_type="text/plain")
        return JSONResponse({**payload("tenant_info"), "cloudId": self.cloud_id})

    async def _myself(self, request: Request, recorded: Recorded, caller: JiraUser) -> Response:
        bean = payload("myself")
        bean.update(caller.bean())
        return JSONResponse(bean)

    async def _server_info(self, request: Request, recorded: Recorded, caller: Any) -> Response:
        info = payload("server_info")
        info["serverTime"] = self._time(self.now_ms)
        return JSONResponse(info)

    async def _search(self, request: Request, recorded: Recorded, caller: JiraUser) -> Response:
        body = recorded.json if isinstance(recorded.json, dict) else {}
        match = _JQL.fullmatch(str(body.get("jql", "")))
        if match is None:
            return JSONResponse(payload("error_bad_jql"), status_code=400)
        since_ms = None
        if match["since"]:
            since = datetime.strptime(match["since"], "%Y/%m/%d %H:%M")
            since_ms = ms_of(since.replace(tzinfo=ZoneInfo(caller.time_zone)))
        hits = sorted(
            (
                issue
                for issue in self.issues.values()
                if issue.indexed
                and match["label"] in issue.labels
                and issue.status != "Done"
                and (since_ms is None or issue.updated_ms >= since_ms)
            ),
            key=lambda issue: (issue.updated_ms, issue.id),
        )
        token = body.get("nextPageToken")
        try:
            offset = int(base64.b64decode(token).decode().removeprefix("offset:")) if token else 0
        except ValueError:
            return JSONResponse(payload("error_bad_jql"), status_code=400)
        size = self._page_size(int(body.get("maxResults", 50)), 5000)
        fields = body.get("fields")
        if isinstance(fields, str):
            fields = [name.strip() for name in fields.split(",")]
        page = hits[offset : offset + size]
        response = payload("search_jql")
        response["issues"] = [self._issue_bean(issue, fields) for issue in page]
        if offset + size < len(hits):
            response["nextPageToken"] = base64.b64encode(
                f"offset:{offset + size}".encode()
            ).decode()
            response["isLast"] = False
        else:
            response.pop("nextPageToken", None)
            response["isLast"] = True
        return JSONResponse(response)

    async def _changelog(self, request: Request, recorded: Recorded, caller: Any) -> Response:
        issue = self._find(request.path_params["issue"])
        if issue is None:
            return self._not_found()
        start = int(recorded.params.get("startAt", "0"))
        size = self._page_size(int(recorded.params.get("maxResults", "100")), 100)
        values = [self._history_bean(entry) for entry in issue.history[start : start + size]]
        last = start + size >= len(issue.history)
        base = f"{SITE}/rest/api/3/issue/{issue.key}/changelog"
        page = payload("changelog_page")
        page.update(
            {
                "self": f"{base}?maxResults={size}&startAt={start}",
                "maxResults": size,
                "startAt": start,
                "total": len(issue.history),
                "isLast": last,
                "values": values,
            }
        )
        if last:
            page.pop("nextPage", None)
        else:
            page["nextPage"] = f"{base}?maxResults={size}&startAt={start + size}"
        return JSONResponse(page)

    async def _comments(self, request: Request, recorded: Recorded, caller: JiraUser) -> Response:
        issue = self._find(request.path_params["issue"])
        if issue is None:
            return self._not_found()
        if request.method == "POST":
            body = recorded.json if isinstance(recorded.json, dict) else {}
            if not isinstance(body.get("body"), dict) or body["body"].get("type") != "doc":
                return JSONResponse(
                    {"errorMessages": [], "errors": {"comment": "Comment body is not valid ADF."}},
                    status_code=400,
                )
            stamp = self._tick()
            comment = JiraComment(
                id=next(self._comment_ids),
                issue_id=issue.id,
                author=caller,
                body=body["body"],
                created_ms=stamp,
                updated_ms=stamp,
                properties={entry["key"]: entry["value"] for entry in body.get("properties") or []},
            )
            issue.comments.append(comment)
            issue.updated_ms = max(issue.updated_ms, stamp)
            return JSONResponse(self._comment_bean(comment), status_code=201)
        order = recorded.params.get("orderBy", "created")
        ordered = sorted(issue.comments, key=lambda c: (c.created_ms, c.id))
        if order == "-created":
            ordered.reverse()
        start = int(recorded.params.get("startAt", "0"))
        size = self._page_size(int(recorded.params.get("maxResults", "5000")), 5000)
        page = payload("comments_page")
        page.update(
            {
                "comments": [self._comment_bean(c) for c in ordered[start : start + size]],
                "maxResults": size,
                "total": len(ordered),
                "startAt": start,
            }
        )
        return JSONResponse(page)

    async def _comment_update(
        self, request: Request, recorded: Recorded, caller: JiraUser
    ) -> Response:
        issue = self._find(request.path_params["issue"])
        if issue is None:
            return self._not_found()
        found = [c for c in issue.comments if str(c.id) == request.path_params["comment"]]
        if not found:
            return self._not_found()
        body = recorded.json if isinstance(recorded.json, dict) else {}
        if not isinstance(body.get("body"), dict) or body["body"].get("type") != "doc":
            return JSONResponse(
                {"errorMessages": [], "errors": {"comment": "Comment body is not valid ADF."}},
                status_code=400,
            )
        comment = found[0]
        comment.body = body["body"]
        comment.updated_ms = self._tick()
        for entry in body.get("properties") or []:
            comment.properties[entry["key"]] = entry["value"]
        return JSONResponse(self._comment_bean(comment))

    async def _comment_list(self, request: Request, recorded: Recorded, caller: Any) -> Response:
        body = recorded.json if isinstance(recorded.json, dict) else {}
        wanted = {str(value) for value in body.get("ids") or []}
        expand = recorded.params.get("expand", "").split(",")
        listed = [
            self._comment_bean(comment, properties="properties" in expand)
            for issue in self.issues.values()
            for comment in issue.comments
            if str(comment.id) in wanted
        ]
        page = payload("comment_list_page")
        page.update({"values": listed, "total": len(listed), "isLast": True, "startAt": 0})
        return JSONResponse(page)

    async def _comment_property(
        self, request: Request, recorded: Recorded, caller: Any
    ) -> Response:
        try:
            comment = self.comment(request.path_params["comment"])
        except KeyError:
            return self._not_found()
        key = request.path_params["key"]
        if request.method == "PUT":
            created = key not in comment.properties
            comment.properties[key] = recorded.json
            return Response(status_code=201 if created else 200)
        if key not in comment.properties:
            return self._not_found()
        return JSONResponse({"key": key, "value": comment.properties[key]})

    async def _issue(self, request: Request, recorded: Recorded, caller: JiraUser) -> Response:
        issue = self._find(request.path_params["issue"])
        if issue is None:
            return self._not_found()
        if request.method == "GET":
            fields = recorded.params.get("fields")
            names = [name.strip() for name in fields.split(",")] if fields else None
            return JSONResponse(self._issue_bean(issue, names))
        if self.forbid_label_edits:
            return JSONResponse(payload("error_forbidden"), status_code=403)
        body = recorded.json if isinstance(recorded.json, dict) else {}
        labels = list(issue.labels)
        for operation in (body.get("update") or {}).get("labels", []):
            if "add" in operation and operation["add"] not in labels:
                labels.append(operation["add"])
            if "remove" in operation and operation["remove"] in labels:
                labels.remove(operation["remove"])
            if "set" in operation:
                labels = list(operation["set"])
        if "labels" in (body.get("fields") or {}):
            labels = list(body["fields"]["labels"])
        if labels != issue.labels:
            self._labels_change(issue, caller, labels, None)
        return Response(status_code=204)

    async def _remote_links(self, request: Request, recorded: Recorded, caller: Any) -> Response:
        issue = self._find(request.path_params["issue"])
        if issue is None:
            return self._not_found()
        if request.method == "GET":
            return JSONResponse(copy.deepcopy(issue.remote_links))
        body = recorded.json if isinstance(recorded.json, dict) else {}
        target = body.get("object") or {}
        if not target.get("url") or not target.get("title"):
            return JSONResponse(
                {"errorMessages": [], "errors": {"object": "url and title are required"}},
                status_code=400,
            )
        global_id = body.get("globalId")
        existing = next(
            (link for link in issue.remote_links if global_id and link["globalId"] == global_id),
            None,
        )
        if existing is not None:
            existing["object"] = copy.deepcopy(target)
            return JSONResponse({"id": existing["id"], "self": existing["self"]})
        link = payload("remote_link")
        link_id = next(self._link_ids)
        link.update(
            {
                "id": link_id,
                "self": f"{SITE}/rest/api/3/issue/{issue.key}/remotelink/{link_id}",
                "globalId": global_id,
                "object": copy.deepcopy(target),
            }
        )
        issue.remote_links.append(link)
        created = payload("remote_link_created")
        created.update({"id": link_id, "self": link["self"]})
        return JSONResponse(created, status_code=201)

    async def _user_groups(self, request: Request, recorded: Recorded, caller: Any) -> Response:
        if self.forbid_group_reads:
            return JSONResponse(payload("error_forbidden"), status_code=403)
        account_id = recorded.params.get("accountId", "")
        if account_id not in self.users:
            return JSONResponse(
                {"errorMessages": ["The user does not exist."], "errors": {}}, status_code=404
            )
        template = payload("user_groups")[0]
        return JSONResponse(
            [
                {
                    **template,
                    "name": f"group-{group_id}",
                    "groupId": group_id,
                    "self": f"{SITE}/rest/api/3/group?groupId={group_id}",
                }
                for group_id, members in sorted(self.groups.items())
                if account_id in members
            ]
        )

    _caller_zone: str = "UTC"


# Config and adapter construction -----------------------------------------------


def jira_tracker_entry(
    *,
    api_url: str = SITE,
    username: str | None = BOT_EMAIL,
    token: str = TOKEN,
    label: str = LABEL,
    mention: str = BOT_ACCOUNT_ID,
    credential: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if credential is None:
        credential = {"type": "static_token", "token": token}
        if username is not None:
            credential["username"] = username
    return {
        "name": TRACKER_NAME,
        "kind": "jira_cloud",
        "host": SITE_HOST,
        "api_url": api_url,
        "credential": credential,
        "label": label,
        "mention": mention,
        "poll_interval_s": 45,
    }


def jira_document(
    *,
    scope_id: str = CLOUD_ID,
    start_authority: dict[str, Any] | None = None,
    **tracker: Any,
) -> dict[str, Any]:
    """A Jira site bound to the GitHub fixture repository (decision 21's shape)."""

    return {
        "code_hosts": [
            {
                "name": CODE_HOST_NAME,
                "kind": "github",
                "host": "github.com",
                "api_url": "https://api.github.com",
                "credential": {
                    "type": "github_app",
                    "app_id": "51",
                    "private_key": "example-private-key",
                },
            }
        ],
        "trackers": [jira_tracker_entry(**tracker)],
        "bindings": [
            {
                "name": BINDING_NAME,
                "tracker": TRACKER_NAME,
                "tracker_scope_id": scope_id,
                "repos": [
                    {
                        "alias": "bot",
                        "code_host": CODE_HOST_NAME,
                        "project_id": GITHUB_REPO_ID,
                        "path": GITHUB_REPO,
                    }
                ],
                "default_repo": "bot",
                "start_authority": start_authority or {"type": "group", "group_id": STARTERS},
            }
        ],
    }


def jira_registry(**document: Any) -> ForgeRegistry:
    return build_registry(ForgesConfig.model_validate(jira_document(**document)))


def fake_client(fake: JiraFake) -> httpx.AsyncClient:
    """A client whose every request is served by ``fake`` over ASGI."""

    return httpx.AsyncClient(transport=httpx.ASGITransport(app=fake.app))


def jira_tracker(
    fake: JiraFake,
    *,
    client: httpx.AsyncClient | None = None,
    label: str | None = None,
    **document: Any,
) -> Any:
    """The Jira tracker the production builder makes for the fixture binding:
    `build_registry` -> `binding_tracker` -> `for_binding`."""

    registry = jira_registry(**document)
    connection = registry.binding_tracker(TRACKER_NAME)
    return connection.for_binding(
        client or fake_client(fake), registry.binding(BINDING_NAME), label=label
    )
