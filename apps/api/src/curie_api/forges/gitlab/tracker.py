"""The GitLab tracker behind the Tracker port (ADR 0197, #4399 decisions 4 and 8 to 15).

One `GitLabTracker` serves one binding: one GitLab project, addressed by its
numeric id (decision 4), the trigger label, and the account ``mention`` names.
Every request goes through one `GitLabTransport`, shared with its marked
comments.

Polling (decision 8). The cursor is opaque to the core and is
``"<T>;<ids>"``: ``T`` is the largest issue ``updated_at`` seen, in UTC ISO
8601, and ``<ids>`` the events a poll already considered whose time is within
the 120 second overlap of ``T``, each namespaced (``label:``, ``note:``)
because label events and notes are separate GitLab sequences that share
numbers. Each window starts 120 seconds before ``T`` and overlaps the last;
the id set keeps the overlap from delivering an event twice, as on Jira. A
redelivery that still happens is absorbed by the ``event_id`` keyed request id
(ADR 0187). ``None`` is the first poll: it lists the open issues carrying the
label and emits only the latest add on each; with no such issue the cursor is
the server's ``Date``.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from curie_api.forges import types
from curie_api.forges.capabilities import TRACKER_OPERATIONS, Operation, Support
from curie_api.forges.errors import NotFound, Unavailable, Unsupported
from curie_api.forges.gitlab.comments import GitLabMarkedComments
from curie_api.forges.gitlab.transport import (
    GitLabTransport,
    actor_of,
    parse_time,
    render_time,
)
from curie_api.forges.types import (
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

TRACKER_CAPABILITIES: Mapping[Operation, Support] = {
    **dict.fromkeys(TRACKER_OPERATIONS, Support.SUPPORTED),
    Operation.LINK_PULL_REQUEST: Support.NOOP,
    Operation.DEPENDENCIES: Support.NOOP,
    Operation.GROUP_MEMBERSHIP: Support.UNSUPPORTED,
}
# The overlap between poll windows (decision 8): it absorbs clock skew and
# issues committed late.
OVERLAP = timedelta(seconds=120)
TICKET_NOTE_LIMIT = 100
LATER_COMMENTS = "_Later comments were not read._"
_LABEL = "label"
_NOTE = "note"
_STATE = "state"
_ID_TOKEN = re.compile(rf"(?:{_LABEL}|{_NOTE}):[0-9]+")


def issue_web_url(web_base: str, issue: TrackerIssueRef, *, host: str) -> str:
    """``<web base>/<path>/-/issues/<iid>`` with no request (decision 14); the
    path is the one ``display_key`` carries. `NotFound` when it cannot be named."""

    if issue.kind != types.GITLAB or issue.host != host or not issue.issue_id.isdigit():
        raise NotFound("issue")
    path, separator, iid = (issue.display_key or "").rpartition("#")
    if not separator or not path or iid != issue.issue_id:
        raise NotFound("issue")
    return f"{web_base}/{path}/-/issues/{issue.issue_id}"


def mention_pattern(mention: str) -> re.Pattern[str]:
    """``@<mention>`` as a whole word (decision 8): not inside an address and
    not the prefix of a longer username."""

    return re.compile(rf"(?<![\w.@-])@{re.escape(mention)}(?![\w.-])", re.IGNORECASE)


@dataclass(frozen=True)
class Cursor:
    """The poll position: ``T`` and the overlap's considered ids."""

    time: datetime
    ids: frozenset[str]

    @classmethod
    def parse(cls, text: str) -> Cursor:
        """The cursor ``text`` encodes; `ValueError` when it is not one. The poll
        pass keeps a cursor that does not parse rather than resetting to a first
        poll, which would drop every add since."""

        time_text, separator, listed = text.partition(";")
        moment = parse_time(time_text)
        if not separator or moment is None:
            raise ValueError("not a gitlab poll cursor")
        ids = frozenset(token for token in listed.split(",") if token)
        if not all(_ID_TOKEN.fullmatch(token) for token in ids):
            raise ValueError("not a gitlab poll cursor")
        return cls(moment, ids)

    def render(self) -> str:
        return f"{render_time(self.time)};{','.join(sorted(self.ids))}"


@dataclass(frozen=True)
class _Pending:
    issue: TrackerIssueRef
    actor: Actor
    event_id: str
    disposition: Disposition
    at: datetime


@dataclass(frozen=True)
class _Event:
    """A label event or a note, normalized."""

    token: str
    at: datetime
    payload: dict[str, Any]


def _label_name(event: Mapping[str, Any]) -> str | None:
    label = event.get("label")
    name = label.get("name") if isinstance(label, dict) else None
    return name if isinstance(name, str) else None


def _labels(issue: Mapping[str, Any]) -> set[str]:
    labels = issue.get("labels")
    if not isinstance(labels, list):
        raise Unavailable("labels")
    return {label for label in labels if isinstance(label, str)}


def _events(kind: str, items: list[dict[str, Any]]) -> list[_Event]:
    found = []
    for item in items:
        at = parse_time(item.get("created_at"))
        if type(item.get("id")) is not int or at is None:
            continue
        found.append(_Event(f"{kind}:{item['id']}", at, item))
    return found


def _latest(events: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    """The newest event by creation time, then id."""

    best: Mapping[str, Any] | None = None
    best_key: tuple[datetime, int] | None = None
    for event in events:
        at = parse_time(event.get("created_at"))
        event_id = event.get("id")
        if at is None or type(event_id) is not int:
            continue
        if best_key is None or (at, event_id) > best_key:
            best, best_key = event, (at, event_id)
    return best


class GitLabTracker:
    """The factory's tracker on one GitLab project, for one binding."""

    kind = types.GITLAB
    capabilities: Mapping[Operation, Support] = TRACKER_CAPABILITIES

    def __init__(
        self,
        transport: GitLabTransport,
        *,
        host: str,
        web_base: str,
        project_id: str,
        project_path: str,
        label: str,
        mention: str,
    ) -> None:
        self._transport = transport
        self.host = host
        self._web_base = web_base
        self._project_id = project_id
        self._project_path = project_path
        self.label = label
        self._mention = mention_pattern(mention)
        self._comments = GitLabMarkedComments(transport, host=host, project_id=project_id)

    def __repr__(self) -> str:
        return f"GitLabTracker(host={self.host!r}, project_id={self._project_id!r})"

    @property
    def marked_comments(self) -> GitLabMarkedComments:
        return self._comments

    async def verify_credential(self) -> None:
        """Read-only credential check for health reporting (#4630): the version
        gate, then the token's own user (``GET /user``). Writes nothing."""

        await self._transport.ensure_ready()
        await self._transport.own_user()

    # References ---------------------------------------------------------------

    def _ref(self, iid: int) -> TrackerIssueRef:
        return TrackerIssueRef(
            types.GITLAB,
            self.host,
            self._project_id,
            str(iid),
            f"{self._project_path}#{iid}",
        )

    def _issue_path(self, issue: TrackerIssueRef) -> str:
        if (
            issue.kind != types.GITLAB
            or issue.host != self.host
            or issue.scope_id != self._project_id
            or not issue.issue_id.isdigit()
        ):
            raise NotFound("issue")
        return f"/projects/{self._project_id}/issues/{issue.issue_id}"

    @property
    def _issues(self) -> str:
        return f"/projects/{self._project_id}/issues"

    async def _read_issue(self, issue: TrackerIssueRef) -> dict[str, Any]:
        return await self._transport.get_object(self._issue_path(issue))

    async def _label_events(self, issue_path: str) -> list[dict[str, Any]]:
        return await self._transport.list_all(f"{issue_path}/resource_label_events")

    async def _notes(self, issue_path: str) -> list[dict[str, Any]]:
        listed = await self._transport.list_all(
            f"{issue_path}/notes", {"sort": "asc", "order_by": "created_at"}
        )
        return [note for note in listed if note.get("system") is not True]

    # Polling --------------------------------------------------------------------

    async def poll_marked(
        self, cursor: str | None, *, running: Sequence[TrackerIssueRef]
    ) -> NoticePage:
        """Decision 8 with the cursor of the module docstring. Every listing
        page is read first, then each issue's label events and notes, then each
        running issue; any failed read raises before a cursor exists."""

        start = None if cursor is None else Cursor.parse(cursor)
        if start is None:
            params: dict[str, str | int] = {
                "labels": self.label,
                "state": "opened",
                "order_by": "updated_at",
                "sort": "asc",
            }
        else:
            params = {
                "updated_after": render_time(start.time - OVERLAP),
                "order_by": "updated_at",
                "sort": "asc",
            }
        listed = await self._transport.list_all(self._issues, params)
        listed_at = self._transport.server_date
        issues: dict[int, datetime] = {}
        for issue in listed:
            iid, updated = issue.get("iid"), parse_time(issue.get("updated_at"))
            if type(iid) is not int or updated is None:
                raise Unavailable("issues")
            issues[iid] = updated
        if start is not None:
            latest = max([start.time, *issues.values()])
        elif issues:
            latest = max(issues.values())
        else:
            latest = listed_at or datetime.now(UTC)
        floor = latest - OVERLAP
        known = frozenset() if start is None else start.ids
        considered: dict[str, datetime] = {}
        pending: list[_Pending] = []
        for iid in issues:
            path = f"{self._issues}/{iid}"
            labels = _events(_LABEL, await self._label_events(path))
            notes = _events(_NOTE, await self._notes(path))
            ref = self._ref(iid)
            if start is None:
                pending.extend(await self._first_poll(ref, labels, notes, floor, considered))
                continue
            window = start.time - OVERLAP
            pending.extend(await self._label_notices(ref, labels, window, known, considered))
            pending.extend(await self._mention_notices(ref, notes, window, known, considered))
        pending.sort(key=lambda item: (item.at, item.event_id))
        delivered = {(item.issue, item.event_id) for item in pending}
        for ref in running:
            cancel = await self._cancellation(ref)
            if cancel is not None and (cancel.issue, cancel.event_id) not in delivered:
                pending.append(cancel)
                delivered.add((cancel.issue, cancel.event_id))
        ids = {token for token, at in considered.items() if at >= floor}
        if start is not None and latest == start.time:
            # The window did not move, so an id this poll did not see is still in it.
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
                    occurred_at=item.at,
                )
                for item in pending
            ),
            cursor=page_cursor,
        )

    async def _first_poll(
        self,
        ref: TrackerIssueRef,
        labels: list[_Event],
        notes: list[_Event],
        floor: datetime,
        considered: dict[str, datetime],
    ) -> list[_Pending]:
        """No cursor: only the latest add of the label is a notice, and only when
        a person made it. Everything else in the overlap is remembered."""

        for event in (*labels, *notes):
            if event.at >= floor:
                considered[event.token] = event.at
        adds = [
            e.payload
            for e in labels
            if e.payload.get("action") == "add" and _label_name(e.payload) == self.label
        ]
        latest = _latest(adds)
        if latest is None:
            return []
        actor = await self._transport.is_person(latest.get("user"))
        at = parse_time(latest.get("created_at"))
        if actor is None or at is None:
            return []
        token = f"{_LABEL}:{latest['id']}"
        considered[token] = at
        return [_Pending(ref, actor, token, Disposition.ADMIT, at)]

    async def _label_notices(
        self,
        ref: TrackerIssueRef,
        events: list[_Event],
        window: datetime,
        known: frozenset[str],
        considered: dict[str, datetime],
    ) -> list[_Pending]:
        notices = []
        for event in events:
            action = event.payload.get("action")
            if event.at < window or _label_name(event.payload) != self.label:
                continue
            if action not in ("add", "remove"):
                continue
            considered[event.token] = event.at
            if event.token in known:
                continue
            actor = await self._transport.is_person(event.payload.get("user"))
            if actor is None:
                continue
            disposition = Disposition.ADMIT if action == "add" else Disposition.CANCEL
            notices.append(_Pending(ref, actor, event.token, disposition, event.at))
        return notices

    async def _mention_notices(
        self,
        ref: TrackerIssueRef,
        notes: list[_Event],
        window: datetime,
        known: frozenset[str],
        considered: dict[str, datetime],
    ) -> list[_Pending]:
        notices = []
        for note in notes:
            body = note.payload.get("body")
            if note.at < window or not isinstance(body, str) or not self._mention.search(body):
                continue
            considered[note.token] = note.at
            if note.token in known:
                continue
            actor = await self._transport.is_person(note.payload.get("author"))
            if actor is not None:
                notices.append(_Pending(ref, actor, note.token, Disposition.MENTION, note.at))
        return notices

    async def _cancellation(self, ref: TrackerIssueRef) -> _Pending | None:
        """A running issue that is closed, or lacks the label, is cancelled with
        the latest close or remove event; a gone issue yields nothing."""

        try:
            path = self._issue_path(ref)
            issue = await self._transport.get_object(path)
        except NotFound:
            return None
        if issue.get("state") == "closed":
            listed = await self._transport.list_all(f"{path}/resource_state_events")
            event = _latest([e for e in listed if e.get("state") == "closed"])
            kind = _STATE
        elif self.label not in _labels(issue):
            listed = await self._label_events(path)
            event = _latest(
                [e for e in listed if e.get("action") == "remove" and _label_name(e) == self.label]
            )
            kind = _LABEL
        else:
            return None
        if event is None:
            return None
        actor = actor_of(event.get("user"))
        at = parse_time(event.get("created_at"))
        if actor is None or at is None:
            return None
        return _Pending(ref, actor, f"{kind}:{event['id']}", Disposition.CANCEL, at)

    async def verify_current(self, notice: MarkedNotice) -> bool:
        """Re-reads the issue every time. A cancellation holds while the label is
        gone or the issue closed; an admission while its add is the latest one
        on the open, labelled issue; a mention while its note stands."""

        path = self._issue_path(notice.issue)
        try:
            issue = await self._transport.get_object(path)
        except NotFound:
            return notice.disposition is Disposition.CANCEL
        labelled = notice.marker in _labels(issue)
        closed = issue.get("state") == "closed"
        if notice.disposition is Disposition.CANCEL:
            return not labelled or closed
        if notice.disposition is Disposition.MENTION:
            return await self._mention_current(path, notice)
        if not labelled or closed:
            return False
        latest = _latest(
            [
                e
                for e in await self._label_events(path)
                if e.get("action") == "add" and _label_name(e) == notice.marker
            ]
        )
        return latest is not None and f"{_LABEL}:{latest['id']}" == notice.event_id

    async def _mention_current(self, path: str, notice: MarkedNotice) -> bool:
        kind, _, note_id = notice.event_id.partition(":")
        if kind != _NOTE or not note_id.isdigit():
            return False
        try:
            note = await self._transport.get_object(f"{path}/notes/{note_id}")
        except NotFound:
            return False
        body = note.get("body")
        return (
            note.get("system") is not True
            and actor_of(note.get("author")) == notice.actor
            and isinstance(body, str)
            and self._mention.search(body) is not None
        )

    async def marking_actor(self, issue: TrackerIssueRef, marker: str) -> Actor | None:
        path = self._issue_path(issue)
        if marker not in _labels(await self._transport.get_object(path)):
            return None
        latest = _latest(
            [
                e
                for e in await self._label_events(path)
                if e.get("action") == "add" and _label_name(e) == marker
            ]
        )
        return None if latest is None else actor_of(latest.get("user"))

    async def may_start(self, issue: TrackerIssueRef, actor: Actor) -> bool:
        """Decision 11: Developer or higher and an active user. A Reporter can
        apply labels, so the label alone never starts a run."""

        self._issue_path(issue)
        return await self._transport.can_write(self._project_id, actor.id)

    async def in_group(self, actor: Actor, group_id: str) -> bool:
        raise Unsupported(Operation.GROUP_MEMBERSHIP)

    # Tickets and labels -----------------------------------------------------------

    async def read_input_body(self, issue: TrackerIssueRef) -> str:
        payload = await self._transport.get_object(self._issue_path(issue))
        body = payload.get("description")
        return body if isinstance(body, str) else ""

    async def read_ticket(self, issue: TrackerIssueRef) -> str:
        """Decision 12: title, description, then up to 100 non-system notes
        oldest first. GitLab markdown passes through unchanged."""

        path = self._issue_path(issue)
        payload = await self._transport.get_object(path)
        title = payload.get("title")
        parts = [f"# {title if isinstance(title, str) else ''}"]
        description = payload.get("description")
        if isinstance(description, str) and description:
            parts.append(description)
        notes: list[dict[str, Any]] = []
        more = False
        async for page in self._transport.pages(
            f"{path}/notes", {"sort": "asc", "order_by": "created_at"}
        ):
            for note in page:
                if note.get("system") is True:
                    continue
                if len(notes) == TICKET_NOTE_LIMIT:
                    more = True
                    break
                notes.append(note)
            if more:
                break
        for note in notes:
            author = actor_of(note.get("author"))
            heading = f"## Comment by {author.login if author is not None else 'unknown'}"
            created = note.get("created_at")
            if isinstance(created, str):
                heading += f" at {created}"
            body = note.get("body")
            parts.append(f"{heading}\n\n{body}" if isinstance(body, str) and body else heading)
        if more:
            parts.append(LATER_COMMENTS)
        return "\n\n".join(parts) + "\n"

    async def ticket_labels(self, issue: TrackerIssueRef) -> TicketLabels:
        return TicketLabels(labels=frozenset(_labels(await self._read_issue(issue))))

    async def set_state_label(
        self, issue: TrackerIssueRef, *, add: str | None, remove: Collection[str]
    ) -> None:
        """Decision 13: one edit with ``add_labels`` and ``remove_labels``, never
        a whole label list that would drop a label a person added meanwhile. A
        refused write (403) is logged and counts as done, as on GitHub."""

        path = self._issue_path(issue)
        dropped = [name for name in remove if name != add]
        if add is None and not dropped:
            return
        change: dict[str, str] = {}
        if add is not None:
            change["add_labels"] = add
        if dropped:
            change["remove_labels"] = ",".join(dropped)
        response = await self._transport.request("PUT", path, json=change, allow=(403,))
        if response.status_code == 403:
            logger.warning(
                "factory state label refused",
                extra={"gitlab_issue_iid": issue.issue_id, "status": 403},
            )

    def issue_url(self, issue: TrackerIssueRef) -> str:
        if issue.scope_id != self._project_id:
            raise NotFound("issue")
        return issue_web_url(self._web_base, issue, host=self.host)

    async def closing_reference(self, issue: TrackerIssueRef, repository: RepositoryRef) -> str:
        """Decision 14: GitLab closes the issue when the merge request merges."""

        self._issue_path(issue)
        return f"Closes #{issue.issue_id}"

    async def publication_references(
        self, issue: TrackerIssueRef, repository: RepositoryRef
    ) -> tuple[str, ...]:
        """``Closes #<iid>`` only when ``repository`` is the issue's own project
        (same kind, host and project id). In any other project a bare ``#<iid>``
        names that project's own issue, so it is never a reference to this one."""

        self._issue_path(issue)
        if (
            repository.kind != types.GITLAB
            or repository.host != issue.host
            or repository.project_id != issue.scope_id
        ):
            return ()
        return (await self.closing_reference(issue, repository),)

    async def link_pull_request(self, issue: TrackerIssueRef, pull_request: PullRequest) -> None:
        """No-op: the merge request's ``Closes #<iid>`` already links it."""

        return None

    async def dependencies(self, issue: TrackerIssueRef) -> tuple[TrackerIssueRef, ...]:
        return ()
