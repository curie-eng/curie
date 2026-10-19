"""The Jira Cloud tracker behind the Tracker port (ADR 0197, #4358).

One `JiraTracker` serves one binding: the site (``host``), the binding's
``tracker_scope_id`` (the site's cloudId, decision 3), the trigger label, the
bot account the tracker's ``mention`` names, and the binding's start authority
(an account allowlist or a group, decision 13). Every read and write goes to
Jira Cloud REST v3 through `curie_api.forges.jira_cloud.transport`.

Identity (decision 3, `curie_api.forges.jira_cloud.identity`): before its
first poll and before every label, comment or link write the tracker checks the
site's cloudId against the binding's scope and the token's accountId against
``mention``, raising `Unauthorized` naming the mismatch, so a wrong scope can
never split one ticket into two WorkItems and a wrong token never writes. The
answer is cached on the connection, so it is read once.

Polling (decisions 6 to 12, plan D2). The cursor is opaque to the core and is
``"<T>;<ids>"``: ``T`` is the largest issue ``updated`` seen, in epoch ms, and
``<ids>`` the events a poll already considered whose ``created`` is within the
120 second overlap of ``T``. JQL dates have minute precision in the bot's time
zone, so each window starts 120 seconds before ``T`` and overlaps the last;
the id set keeps the overlap from delivering an event twice. History ids and
comment ids are separate Jira sequences that share numbers, so each id is
stored with its namespace (``h`` for a changelog history, ``c`` for a comment,
``i`` for an issue's creation). ``None`` is the first poll: only the latest
label add of each labelled issue, or its creation when no add exists, is a
notice. A redelivery that still happens (an event that left both window and
set) is absorbed by the ``event_id`` keyed request id (ADR 0187).
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from curie_api.forges.capabilities import TRACKER_OPERATIONS, Operation, Support
from curie_api.forges.config import AccountAllowlist, GroupAuthority, StartAuthority
from curie_api.forges.errors import NotFound, Unavailable
from curie_api.forges.jira_cloud import adf
from curie_api.forges.jira_cloud.comments import JiraMarkedComments
from curie_api.forges.jira_cloud.identity import IdentityCache, verify_identity
from curie_api.forges.jira_cloud.transport import Forbidden, JiraTransport
from curie_api.forges.types import (
    JIRA_CLOUD,
    Actor,
    Disposition,
    MarkedNotice,
    NoticePage,
    PullRequest,
    RepositoryRef,
    TicketLabels,
    TrackerIssueRef,
)

logger = logging.getLogger(__name__)

# The overlap between windows: JQL `updated` has minute precision (decision 7).
WINDOW_MS = 120_000
SEARCH_FIELDS = ["labels", "status", "components", "updated", "summary", "creator", "created"]
TICKET_COMMENT_LIMIT = 100
LATER_COMMENTS = "_Later comments were not read._"
_HISTORY = "h"
_COMMENT = "c"
_CREATION = "i"
_ID_TOKEN = re.compile(rf"(?:[{_HISTORY}{_COMMENT}][0-9]+|{_CREATION}[0-9]+:[0-9]+)")
_COMMENT_SELF = re.compile(r"/issue/([0-9]+)/comment/")


@dataclass(frozen=True)
class Cursor:
    """The poll position (plan D2): ``T`` and the overlap's considered ids."""

    time_ms: int
    ids: frozenset[str]

    @classmethod
    def parse(cls, text: str) -> Cursor:
        """The cursor ``text`` encodes; `ValueError` when it is not one.

        The poll pass keeps a cursor that does not parse rather than resetting
        to a first poll, which would drop every add since.
        """

        time_text, separator, listed = text.partition(";")
        if not separator or not time_text.isascii() or not time_text.isdigit():
            raise ValueError("not a jira_cloud poll cursor")
        ids = frozenset(token for token in listed.split(",") if token)
        if not all(_ID_TOKEN.fullmatch(token) for token in ids):
            raise ValueError("not a jira_cloud poll cursor")
        return cls(int(time_text), ids)

    def render(self) -> str:
        return f"{self.time_ms};{','.join(sorted(self.ids))}"


def jira_ms(value: Any) -> int | None:
    """A Jira date-time (``2026-03-08T01:59:30.000-0500``) in epoch ms, or None."""

    if not isinstance(value, str):
        return None
    for layout in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return int(datetime.strptime(value, layout).timestamp() * 1000)
        except ValueError:
            continue
    return None


def _actor(user: Any) -> Actor | None:
    if not isinstance(user, dict):
        return None
    account_id = user.get("accountId")
    if not isinstance(account_id, str) or not account_id:
        return None
    name = user.get("displayName")
    return Actor(id=account_id, login=name if isinstance(name, str) and name else account_id)


def _label_tokens(value: Any) -> set[str]:
    return set(value.split()) if isinstance(value, str) else set()


def _field_items(history: Mapping[str, Any], field: str) -> list[Mapping[str, Any]]:
    items = history.get("items")
    if not isinstance(items, list):
        return []
    return [
        item
        for item in items
        if isinstance(item, dict) and (item.get("field") == field or item.get("fieldId") == field)
    ]


def adds_label(history: Mapping[str, Any], label: str) -> bool:
    """The changelog history added ``label`` (decision 6)."""

    return any(
        label in _label_tokens(item.get("toString"))
        and label not in _label_tokens(item.get("fromString"))
        for item in _field_items(history, "labels")
    )


def removes_label(history: Mapping[str, Any], label: str) -> bool:
    return any(
        label in _label_tokens(item.get("fromString"))
        and label not in _label_tokens(item.get("toString"))
        for item in _field_items(history, "labels")
    )


def changes_status(history: Mapping[str, Any]) -> bool:
    return bool(_field_items(history, "status"))


def mentions(node: Any, account_id: str) -> bool:
    """An ADF ``mention`` node in ``node`` names ``account_id`` (decision 11)."""

    if isinstance(node, list):
        return any(mentions(child, account_id) for child in node)
    if not isinstance(node, dict):
        return False
    if node.get("type") == "mention":
        attrs = node.get("attrs")
        if isinstance(attrs, dict) and attrs.get("id") == account_id:
            return True
    return mentions(node.get("content"), account_id)


def _fields(issue: Any) -> Mapping[str, Any]:
    fields = issue.get("fields") if isinstance(issue, dict) else None
    return fields if isinstance(fields, dict) else {}


def _labels(fields: Mapping[str, Any]) -> set[str]:
    labels = fields.get("labels")
    if not isinstance(labels, list):
        raise Unavailable("labels")
    return {label for label in labels if isinstance(label, str)}


def _done(fields: Mapping[str, Any]) -> bool:
    status = fields.get("status")
    category = status.get("statusCategory") if isinstance(status, dict) else None
    return isinstance(category, dict) and category.get("key") == "done"


def browse_url(site_origin: str, issue: TrackerIssueRef, *, host: str) -> str:
    """``<site>/browse/<key>`` with no request (decision 18); Jira redirects a moved key."""

    if issue.kind != JIRA_CLOUD or issue.host != host or not issue.display_key:
        raise NotFound("issue")
    return f"{site_origin}/browse/{issue.display_key}"


def _from_ms(value: int | None) -> datetime | None:
    return None if value is None else datetime.fromtimestamp(value / 1000, UTC)


def _key_moves(histories: Sequence[Any]) -> list[str]:
    """Every key the issue was moved away from, oldest first, read strictly.

    A key change is a changelog item whose ``field`` is ``Key`` with the old
    key in ``fromString``
    (https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-issueidorkey-changelog-get,
    ``ChangeDetails``). A history or item list that cannot be read, or a key
    change without its old key, raises `Unavailable`.
    """

    keys: list[str] = []
    for history in histories:
        items = history.get("items") if isinstance(history, dict) else None
        if not isinstance(items, list):
            raise Unavailable("changelog")
        for item in items:
            if not isinstance(item, dict):
                raise Unavailable("changelog")
            if item.get("field") != "Key" and item.get("fieldId") != "key":
                continue
            old = item.get("fromString")
            if not isinstance(old, str) or not old.strip() or any(c.isspace() for c in old):
                raise Unavailable("changelog")
            keys.append(old)
    return keys


@dataclass(frozen=True)
class _Pending:
    issue: TrackerIssueRef
    actor: Actor
    event_id: str
    disposition: Disposition
    # When Jira says the event happened (epoch ms), or None when it said nothing usable.
    at: int | None


class JiraTracker:
    """The factory's tracker on one Jira Cloud site scope, for one binding."""

    kind = JIRA_CLOUD
    capabilities: Mapping[Operation, Support] = {
        **dict.fromkeys(TRACKER_OPERATIONS, Support.SUPPORTED),
        # No core caller reads dependencies yet (ADR 0165 is a Draft, decision 19).
        Operation.DEPENDENCIES: Support.NOOP,
    }

    def __init__(
        self,
        transport: JiraTransport,
        *,
        host: str,
        site_origin: str,
        scope_id: str,
        label: str,
        mention: str,
        start_authority: StartAuthority,
        identity: IdentityCache,
        page_size: int = 100,
    ) -> None:
        self._transport = transport
        self.host = host
        self._site_origin = site_origin
        self._scope_id = scope_id
        self.label = label
        self._mention = mention
        self._authority = start_authority
        self._identity = identity
        self._page_size = page_size
        self._comments = JiraMarkedComments(
            transport,
            host=host,
            site_origin=site_origin,
            scope_id=scope_id,
            mention=mention,
            identity=identity,
            page_size=page_size,
        )

    @property
    def marked_comments(self) -> JiraMarkedComments:
        return self._comments

    # References ---------------------------------------------------------

    def _ref(self, issue: Mapping[str, Any]) -> TrackerIssueRef | None:
        issue_id = issue.get("id")
        key = issue.get("key")
        if not isinstance(issue_id, str | int) or not str(issue_id).isdigit():
            return None
        return TrackerIssueRef(
            JIRA_CLOUD,
            self.host,
            self._scope_id,
            str(issue_id),
            key if isinstance(key, str) and key else None,
        )

    def _issue_id(self, issue: TrackerIssueRef) -> str:
        if (
            issue.kind != JIRA_CLOUD
            or issue.host != self.host
            or issue.scope_id != self._scope_id
            or not issue.issue_id.isdigit()
        ):
            raise NotFound("issue")
        return issue.issue_id

    def _human(self, user: Any) -> Actor | None:
        """The author when a person other than the bot (decision 10), else None."""

        if not isinstance(user, dict) or user.get("accountType") != "atlassian":
            return None
        actor = _actor(user)
        return None if actor is None or actor.id == self._mention else actor

    def _creation_notice(
        self,
        ref: TrackerIssueRef,
        fields: Mapping[str, Any],
        histories: Sequence[Mapping[str, Any]],
        marker: str,
    ) -> _Pending | None:
        if any(adds_label(history, marker) for history in histories):
            return None
        if (
            marker not in _labels(fields)
            or _done(fields)
            or not ref.issue_id.isascii()
            or not ref.issue_id.isdigit()
        ):
            return None
        created = jira_ms(fields.get("created"))
        actor = self._human(fields.get("creator"))
        if created is None or created < 0 or actor is None:
            return None
        return _Pending(
            ref, actor, f"creation:{ref.issue_id}:{created}", Disposition.ADMIT, created
        )

    # Reads ----------------------------------------------------------------

    async def _read_issue(self, issue_id: str, fields: str) -> Mapping[str, Any]:
        # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-issueidorkey-get
        issue = await self._transport.get(
            f"/issue/{issue_id}", what="issue", params={"fields": fields}
        )
        if not isinstance(issue, dict):
            raise Unavailable("issue")
        return issue

    async def _changelog(self, issue_id: str) -> list[dict[str, Any]]:
        histories = await self._transport.changelog(
            issue_id, page_size=self._page_size, strict=True
        )
        for history in histories:
            items = history.get("items")
            if not isinstance(items, list):
                raise Unavailable("changelog")
            for item in items:
                if not isinstance(item, dict) or not any(
                    isinstance(item.get(key), str) and item[key].strip()
                    for key in ("field", "fieldId")
                ):
                    raise Unavailable("changelog")
                if item.get("field") != "labels" and item.get("fieldId") != "labels":
                    continue
                for key in ("fromString", "toString"):
                    if key not in item or (
                        item[key] is not None and not isinstance(item[key], str)
                    ):
                        raise Unavailable("changelog")
        return histories

    def _latest(
        self, histories: Sequence[Mapping[str, Any]], matches: Callable[[Mapping[str, Any]], bool]
    ) -> Mapping[str, Any] | None:
        found = None
        for history in histories:
            if matches(history):
                found = history
        return found

    async def _verify_identity(self) -> ZoneInfo:
        """Decision 3: the site is the binding's cloudId and the token is the bot."""

        return await verify_identity(
            self._transport,
            self._identity,
            site_origin=self._site_origin,
            scope_id=self._scope_id,
            mention=self._mention,
        )

    async def verify_credential(self) -> None:
        """Read-only credential check for health reporting (#4630): the identity
        read of decision 3 with an empty cache, so it always asks the site and
        the token now, never an identity verified earlier. Writes nothing."""

        await verify_identity(
            self._transport,
            IdentityCache(),
            site_origin=self._site_origin,
            scope_id=self._scope_id,
            mention=self._mention,
        )

    # Tracker port -------------------------------------------------------

    def _jql(self, start: Cursor | None, zone: ZoneInfo) -> str:
        label = self.label.replace("\\", "\\\\").replace('"', '\\"')
        jql = f'labels = "{label}" AND statusCategory != Done'
        if start is not None and start.time_ms > 0:
            since = datetime.fromtimestamp((start.time_ms - WINDOW_MS) / 1000, tz=zone)
            jql += f' AND updated >= "{since.strftime("%Y/%m/%d %H:%M")}"'
        return jql + " ORDER BY updated ASC"

    async def poll_marked(
        self, cursor: str | None, *, running: Sequence[TrackerIssueRef]
    ) -> NoticePage:
        """Decisions 6 to 12 with the cursor of plan D2 (module docstring).

        Every search page is read first, then each issue's whole changelog, then
        its comments back to the window start, then each running issue; any
        failed read raises before a cursor exists.
        """

        start = None if cursor is None else Cursor.parse(cursor)
        zone = await self._verify_identity()
        found = await self._transport.search_jql(
            self._jql(start, zone), SEARCH_FIELDS, page_size=self._page_size
        )
        issues: dict[str, tuple[TrackerIssueRef, int, Mapping[str, Any]]] = {}
        for issue in found:
            ref = self._ref(issue)
            fields = _fields(issue)
            updated = jira_ms(fields.get("updated"))
            if ref is None or updated is None:
                raise Unavailable("search")
            issues.pop(ref.issue_id, None)
            issues[ref.issue_id] = (ref, updated, fields)
        previous = 0 if start is None else start.time_ms
        latest = max([previous, *(updated for _, updated, _issue_fields in issues.values())])
        # Candidates are events at or after the window start; with no window
        # (a first poll, or nothing seen yet) every event is one.
        window = previous - WINDOW_MS if start is not None and previous > 0 else None
        floor = latest - WINDOW_MS
        known = frozenset() if start is None else start.ids
        histories = {issue_id: await self._changelog(issue_id) for issue_id in issues}
        read_back = floor if start is None else window
        comments: dict[str, list[dict[str, Any]]] = {}
        for issue_id in issues:
            comments[issue_id] = await self._recent_comments(issue_id, read_back)
        considered: dict[str, int] = {}
        pending: list[_Pending] = []
        for issue_id, (ref, _updated, fields) in issues.items():
            pending.extend(
                self._label_notices(
                    ref, fields, histories[issue_id], start, window, known, considered
                )
            )
            pending.extend(
                self._mention_notices(ref, comments[issue_id], start, window, known, considered)
            )
        for ref in running:
            cancel = await self._cancellation(ref)
            if cancel is not None:
                pending.append(cancel)
        ids = {token for token, created in considered.items() if created >= floor}
        if latest == previous:
            # The window did not move, so an id this poll did not see (its issue
            # left the search) is still inside it.
            ids |= known - considered.keys()
        page_cursor = Cursor(latest, frozenset(ids)).render()
        return NoticePage(
            notices=tuple(
                MarkedNotice(
                    issue=item.issue,
                    marker=self.label,
                    actor=item.actor,
                    event_id=item.event_id,
                    disposition=item.disposition,
                    cursor=page_cursor,
                    occurred_at=_from_ms(item.at),
                )
                for item in pending
            ),
            cursor=page_cursor,
        )

    async def _recent_comments(self, issue_id: str, floor: int | None) -> list[dict[str, Any]]:
        """The issue's comments newest first, back to ``floor`` (decision 11)."""

        def older(comment: dict[str, Any]) -> bool:
            created = jira_ms(comment.get("created"))
            return floor is not None and created is not None and created < floor

        listed, _total = await self._transport.comments(
            issue_id, newest_first=True, page_size=self._page_size, until=older
        )
        return listed

    def _label_notices(
        self,
        ref: TrackerIssueRef,
        fields: Mapping[str, Any],
        histories: Sequence[Mapping[str, Any]],
        start: Cursor | None,
        window: int | None,
        known: frozenset[str],
        considered: dict[str, int],
    ) -> list[_Pending]:
        creation = self._creation_notice(ref, fields, histories, self.label)
        if creation is not None and creation.at is not None:
            if window is not None and creation.at < window:
                return []
            token = f"{_CREATION}{ref.issue_id}:{creation.at}"
            considered[token] = creation.at
            return [] if token in known else [creation]
        adds: list[tuple[str, int, Actor]] = []
        for history in histories:
            history_id = history.get("id")
            created = jira_ms(history.get("created"))
            if not adds_label(history, self.label) or created is None:
                continue
            if not isinstance(history_id, str | int) or not str(history_id).isdigit():
                continue
            actor = self._human(history.get("author"))
            if start is None:
                # First poll: only the latest add is a notice, and only when a
                # person made it. Earlier adds are remembered, not delivered.
                adds = [] if actor is None else [(str(history_id), created, actor)]
                if actor is not None:
                    considered[f"{_HISTORY}{history_id}"] = created
                continue
            if actor is None or (window is not None and created < window):
                continue
            adds.append((str(history_id), created, actor))
        notices: list[_Pending] = []
        for history_id, created, actor in adds:
            token = f"{_HISTORY}{history_id}"
            considered[token] = created
            if token in known:
                continue
            notices.append(_Pending(ref, actor, history_id, Disposition.ADMIT, created))
        return notices

    def _mention_notices(
        self,
        ref: TrackerIssueRef,
        comments: Sequence[Mapping[str, Any]],
        start: Cursor | None,
        window: int | None,
        known: frozenset[str],
        considered: dict[str, int],
    ) -> list[_Pending]:
        notices: list[_Pending] = []
        for comment in reversed(comments):
            comment_id = comment.get("id")
            created = jira_ms(comment.get("created"))
            actor = self._human(comment.get("author"))
            if (
                actor is None
                or created is None
                or not isinstance(comment_id, str | int)
                or not str(comment_id).isdigit()
                or not mentions(comment.get("body"), self._mention)
            ):
                continue
            token = f"{_COMMENT}{comment_id}"
            if start is None:
                # A first poll delivers label adds only; the mention is remembered.
                considered[token] = created
                continue
            if window is not None and created < window:
                continue
            considered[token] = created
            if token not in known:
                notices.append(_Pending(ref, actor, str(comment_id), Disposition.MENTION, created))
        return notices

    async def _cancellation(self, ref: TrackerIssueRef) -> _Pending | None:
        """Decision 12: a running issue without the label, or done, is cancelled."""

        try:
            issue_id = self._issue_id(ref)
            fields = _fields(await self._read_issue(issue_id, "labels,status"))
        except NotFound:
            return None
        labelled = self.label in _labels(fields)
        if labelled and not _done(fields):
            return None
        histories = await self._changelog(issue_id)
        if not labelled:
            event = self._latest(histories, lambda h: removes_label(h, self.label))
        else:
            event = self._latest(histories, changes_status)
        if event is None:
            return None
        actor = _actor(event.get("author"))
        event_id = event.get("id")
        if actor is None or not isinstance(event_id, str | int) or not str(event_id):
            return None
        return _Pending(
            ref, actor, str(event_id), Disposition.CANCEL, jira_ms(event.get("created"))
        )

    async def verify_current(self, notice: MarkedNotice) -> bool:
        """Re-reads the issue every time (decision 12). An admission or a
        mention is current only while the issue keeps its label and is not done,
        so a mention after the label went, or after done, revises nothing."""

        issue_id = self._issue_id(notice.issue)
        try:
            fields = _fields(await self._read_issue(issue_id, "labels,status,creator,created"))
        except NotFound:
            return notice.disposition is Disposition.CANCEL
        labelled = notice.marker in _labels(fields)
        done = _done(fields)
        if notice.disposition is Disposition.CANCEL:
            return not labelled or done
        if not labelled or done:
            return False
        if notice.disposition is Disposition.MENTION:
            return await self._mention_current(issue_id, notice)
        histories = await self._changelog(issue_id)
        latest = self._latest(histories, lambda h: adds_label(h, notice.marker))
        if latest is not None:
            return str(latest.get("id")) == notice.event_id
        creation = self._creation_notice(notice.issue, fields, histories, notice.marker)
        return (
            creation is not None
            and creation.event_id == notice.event_id
            and creation.actor == notice.actor
        )

    async def _mention_current(self, issue_id: str, notice: MarkedNotice) -> bool:
        if not notice.event_id.isdigit():
            return False
        # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/#api-rest-api-3-comment-list-post
        page = await self._transport.post(
            "/comment/list", what="comment_list", json={"ids": [int(notice.event_id)]}
        )
        values = page.get("values") if isinstance(page, dict) else None
        if not isinstance(values, list):
            raise Unavailable("comment_list")
        for comment in values:
            if not isinstance(comment, dict) or str(comment.get("id")) != notice.event_id:
                continue
            located = _COMMENT_SELF.search(str(comment.get("self", "")))
            author = _actor(comment.get("author"))
            return (
                located is not None
                and located.group(1) == issue_id
                and author == notice.actor
                and mentions(comment.get("body"), self._mention)
            )
        return False

    async def marking_actor(self, issue: TrackerIssueRef, marker: str) -> Actor | None:
        issue_id = self._issue_id(issue)
        fields = _fields(await self._read_issue(issue_id, "labels,status,creator,created"))
        if marker not in _labels(fields):
            return None
        histories = await self._changelog(issue_id)
        latest = self._latest(histories, lambda h: adds_label(h, marker))
        if latest is not None:
            return _actor(latest.get("author"))
        creation = self._creation_notice(issue, fields, histories, marker)
        return None if creation is None else creation.actor

    async def may_start(self, issue: TrackerIssueRef, actor: Actor) -> bool:
        """The binding's start authority (decision 13): an allowlisted account id,
        or a member of the binding's group. A Jira binding never has a write check."""

        self._issue_id(issue)
        if isinstance(self._authority, AccountAllowlist):
            return actor.id in self._authority.accounts
        if isinstance(self._authority, GroupAuthority):
            return await self.in_group(actor, self._authority.group_id)
        return False

    async def in_group(self, actor: Actor, group_id: str) -> bool:
        """Whether ``actor`` is in the group with immutable id ``group_id``.

        https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-users/#api-rest-api-3-user-groups-get
        A group read the bot may not make is `Unauthorized`, so it defers.
        """

        try:
            groups = await self._transport.get(
                "/user/groups", what="user_groups", params={"accountId": actor.id}
            )
        except NotFound:
            return False
        if not isinstance(groups, list):
            raise Unavailable("user_groups")
        return any(isinstance(group, dict) and group.get("groupId") == group_id for group in groups)

    async def read_input_body(self, issue: TrackerIssueRef) -> str:
        fields = _fields(await self._read_issue(self._issue_id(issue), "description"))
        return adf.to_markdown(fields.get("description"))

    async def read_ticket(self, issue: TrackerIssueRef) -> str:
        """Decision 14: title, description, then up to 100 comments oldest first."""

        issue_id = self._issue_id(issue)
        fields = _fields(await self._read_issue(issue_id, "summary,description"))
        summary = fields.get("summary")
        parts = [f"# {summary if isinstance(summary, str) else ''}"]
        description = adf.to_markdown(fields.get("description"))
        if description:
            parts.append(description)
        comments, total = await self._transport.comments(
            issue_id,
            newest_first=False,
            page_size=self._page_size,
            limit=TICKET_COMMENT_LIMIT,
        )
        for comment in comments:
            author = comment.get("author")
            name = author.get("displayName") if isinstance(author, dict) else None
            heading = f"## Comment by {name if isinstance(name, str) else 'unknown'}"
            created = comment.get("created")
            if isinstance(created, str):
                heading += f" at {created}"
            body = adf.to_markdown(comment.get("body"))
            parts.append(f"{heading}\n\n{body}" if body else heading)
        if total > len(comments):
            parts.append(LATER_COMMENTS)
        return "\n\n".join(parts) + "\n"

    async def ticket_labels(self, issue: TrackerIssueRef) -> TicketLabels:
        issue_id = self._issue_id(issue)
        fields = _fields(await self._read_issue(issue_id, "labels,components"))
        components = fields.get("components")
        names = (
            {
                component["name"]
                for component in components
                if isinstance(component, dict) and isinstance(component.get("name"), str)
            }
            if isinstance(components, list)
            else set()
        )
        return TicketLabels(labels=frozenset(_labels(fields)), components=frozenset(names))

    async def set_state_label(
        self, issue: TrackerIssueRef, *, add: str | None, remove: Collection[str]
    ) -> None:
        """Decision 16: one edit of label operations, never a whole label value
        that would drop a label a person added meanwhile. A refused write (403)
        is logged and counts as done, as on GitHub."""

        issue_id = self._issue_id(issue)
        operations: list[dict[str, str]] = [] if add is None else [{"add": add}]
        operations.extend({"remove": name} for name in remove if name != add)
        if not operations:
            return
        await self._verify_identity()
        try:
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-issueidorkey-put
            await self._transport.put(
                f"/issue/{issue_id}",
                what="issue_update",
                json={"update": {"labels": operations}},
            )
        except Forbidden:
            logger.warning(
                "factory state label refused", extra={"jira_issue_id": issue_id, "status": 403}
            )

    def issue_url(self, issue: TrackerIssueRef) -> str:
        if issue.scope_id != self._scope_id:
            raise NotFound("issue")
        return browse_url(self._site_origin, issue, host=self.host)

    async def closing_reference(self, issue: TrackerIssueRef, repository: RepositoryRef) -> str:
        """Decision 17: ``Jira: <KEY> <url>`` with the key read by id now, so a
        moved issue is referenced by its current key."""

        issue_id = self._issue_id(issue)
        key = (await self._read_issue(issue_id, "summary")).get("key")
        if not isinstance(key, str) or not key:
            raise Unavailable("issue")
        return self._reference(key)

    async def publication_references(
        self, issue: TrackerIssueRef, repository: RepositoryRef
    ) -> tuple[str, ...]:
        """``Jira: <KEY> <site>/browse/<KEY>`` (the closing reference) for the
        current key and for every key the issue held before a move, read by its
        immutable id. A Jira key names one issue on the site whatever the
        repository, so each is scope-qualified. Any part of the issue or its
        changelog that cannot be read raises `Unavailable`."""

        issue_id = self._issue_id(issue)
        current = (await self._read_issue(issue_id, "summary")).get("key")
        if not isinstance(current, str) or not current:
            raise Unavailable("issue")
        histories = await self._transport.changelog(
            issue_id, page_size=self._page_size, strict=True
        )
        keys = dict.fromkeys([current, *reversed(_key_moves(histories))])
        return tuple(self._reference(key) for key in keys)

    def _reference(self, key: str) -> str:
        return f"Jira: {key} {self._site_origin}/browse/{key}"

    async def link_pull_request(self, issue: TrackerIssueRef, pull_request: PullRequest) -> None:
        """Decision 17: one remote link keyed by the pull request URL (``globalId``),
        so a second call updates the same link."""

        issue_id = self._issue_id(issue)
        await self._verify_identity()
        number = pull_request.ref.number
        title = f"PR #{number}: {pull_request.title}" if pull_request.title else f"PR #{number}"
        # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-remote-links/#api-rest-api-3-issue-issueidorkey-remotelink-post
        await self._transport.post(
            f"/issue/{issue_id}/remotelink",
            what="remote_link",
            json={
                "globalId": pull_request.url,
                "object": {"url": pull_request.url, "title": title},
            },
        )

    async def dependencies(self, issue: TrackerIssueRef) -> tuple[TrackerIssueRef, ...]:
        return ()
