"""The one-ticket scenario's tracker adapter for Jira (#5791).

The one-ticket assertions (``one_ticket_scenario``) read GitHub-shaped records:
issues with ``number`` and ``labels``, comments with ``issue_number``, ``body``
and ``user.login``, and the history of a comment's bodies. This module turns
what the Jira stub (``tools/jira-stub/jira_stub.py``) reports from outside into
those records, so the same functions judge a Jira ticket:

1. a Jira comment's body is ADF; it is read the way a person reads it in Jira
   (``adf_text``, written here from the ADF reference, not the product's own
   converter), with ordered list items numbered and a link shown as
   ``[text](href)``;
2. the factory's marker is not in a Jira body but in the comment entity
   property ``curie.factory`` (``{"marker": ...}``); a marked comment's record
   starts with that marker as the hidden ``<!-- marker -->`` line the GitHub
   shape carries;
3. a comment's author is its ``accountId``; a Jira issue's number is its id.

Two outcomes are Jira's own and are judged here: the pull request names the
ticket as ``<KEY>`` and its browse URL rather than ``Closes #n``, and the
factory's final status comment is known from the platform's record of it
(``factory_terminal_notices.finalized_at``), because Jira has no hidden text
for the final marker.

The constants below are the Jira fake's (``apps/api/tests/forge_fakes/jira.py``);
``tools/factory-e2e/tests/test_one_ticket_jira.py`` pins them equal. They are
repeated because the scripted driver runs on the system interpreter, without
the fake's dependencies.
"""

from __future__ import annotations

import json
import re
import ssl
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import one_ticket_scenario as ots

# The Jira fake's site identity (placeholders, never a real site or token).
CLOUD_ID = "11111111-1111-1111-1111-111111111111"
BOT_ACCOUNT_ID = "712020:00000000-0000-4000-8000-0000000000b0"
BOT_EMAIL = "factory-bot@example.com"
BOT_TOKEN = "jira-fixture-token-must-never-be-logged-7f3a"

# The people of the scenario.
LABELLER = {"account_id": "5b10a0000000000000000a11", "display_name": "Ada Labeller"}
OUTSIDER = {"account_id": "5b10a0000000000000000a22", "display_name": "Oscar Outsider"}

PROPERTY_KEY = "curie.factory"
EVIDENCE_KEY = "jira_one_ticket"
TOKEN_HEADER = "X-Curie-Stub-Token"


class JiraStubError(RuntimeError):
    """The Jira stub refused or could not answer a control request."""


# The stub's control surface ------------------------------------------------------


class JiraStubClient:
    """The token-bound ``/_stub/`` routes of one running Jira stub, over its TLS."""

    def __init__(self, base_url: str, ca_file: Path, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._context = ssl.create_default_context(cafile=str(ca_file))
        self._token = token

    def _call(self, method: str, path: str, body: Mapping[str, Any] | None = None) -> Any:
        request = Request(
            f"{self.base_url}{path}",
            data=None if body is None else json.dumps(body).encode(),
            headers={TOKEN_HEADER: self._token, "Content-Type": "application/json"},
            method=method,
        )
        try:
            with urlopen(request, context=self._context, timeout=30) as response:
                return json.load(response)
        except HTTPError as exc:
            raise JiraStubError(f"{method} {path} answered {exc.code}") from None

    def add_user(self, person: Mapping[str, str]) -> None:
        self._call("POST", "/_stub/users", dict(person))

    def create_issue(self, summary: str, description: str, reporter: str) -> dict[str, str]:
        created: dict[str, str] = self._call(
            "POST",
            "/_stub/issues",
            {"summary": summary, "description": description, "reporter": reporter},
        )
        return created

    def add_label(self, issue_id: str, label: str, by: str) -> dict[str, str]:
        added: dict[str, str] = self._call(
            "POST", f"/_stub/issues/{issue_id}/labels", {"label": label, "by": by}
        )
        return added

    def add_comment(self, issue_id: str, text: str, author: str) -> dict[str, str]:
        posted: dict[str, str] = self._call(
            "POST", f"/_stub/issues/{issue_id}/comments", {"text": text, "author": author}
        )
        return posted

    def state(self) -> dict[str, Any]:
        observed: dict[str, Any] = self._call("GET", "/_stub/state")
        return observed


# Reading ADF ---------------------------------------------------------------------


def _children(node: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    content = node.get("content")
    return (
        [child for child in content if isinstance(child, Mapping)]
        if isinstance(content, list)
        else []
    )


def _inline(nodes: Iterable[Mapping[str, Any]]) -> str:
    out: list[str] = []
    for node in nodes:
        kind = node.get("type")
        if kind == "text":
            text = str(node.get("text", ""))
            for mark in node.get("marks") or []:
                if isinstance(mark, Mapping) and mark.get("type") == "link":
                    href = (mark.get("attrs") or {}).get("href")
                    if isinstance(href, str):
                        text = f"[{text}]({href})"
            out.append(text)
        elif kind == "hardBreak":
            out.append("\n")
        elif kind == "mention":
            out.append(str((node.get("attrs") or {}).get("text", "")))
        elif kind == "inlineCard":
            out.append(str((node.get("attrs") or {}).get("url", "")))
        else:
            out.append(_inline(_children(node)))
    return "".join(out)


def _blocks(node: Mapping[str, Any]) -> list[str]:
    """``node`` as the lines a reader sees, one entry per block."""

    kind = node.get("type")
    if kind in {"paragraph", "heading"}:
        return [_inline(_children(node))]
    if kind == "codeBlock":
        return [_inline(_children(node))]
    if kind in {"orderedList", "bulletList"}:
        start = (node.get("attrs") or {}).get("order", 1)
        number = start if isinstance(start, int) else 1
        lines: list[str] = []
        for item in _children(node):
            inner = [line for child in _children(item) for line in _blocks(child)]
            lead = f"{number}. " if kind == "orderedList" else "- "
            lines.append(lead + (inner[0] if inner else ""))
            lines.extend("   " + line for line in inner[1:])
            number += 1
        return lines
    if kind == "rule":
        return ["---"]
    return [line for child in _children(node) for line in _blocks(child)]


def adf_text(document: Any) -> str:
    """An ADF document as a person reads it, one block per line.

    Node types: https://developer.atlassian.com/cloud/jira/platform/apis/document/structure/
    """

    if not isinstance(document, Mapping):
        return ""
    return "\n".join(_blocks(document))


# Records in the one-ticket shape -------------------------------------------------


def marker_of(properties: Any) -> str | None:
    value = properties.get(PROPERTY_KEY) if isinstance(properties, Mapping) else None
    marker = value.get("marker") if isinstance(value, Mapping) else None
    return marker if isinstance(marker, str) and marker else None


# The factory's marker prefixes. On Jira a marker is only ever the comment
# property; the same words typed into a comment's text mark nothing.
MARKER_PREFIXES = (ots.QUESTIONS_MARKER, "curie-execution-request:")


def inert(text: str) -> str:
    """``text`` with every marker prefix it shows broken, so visible words can
    never stand in for the property the assertions require."""

    for prefix in MARKER_PREFIXES:
        text = text.replace(prefix, f"{prefix[:-1]} (visible text):")
    return text


def rendered(body: Any, properties: Any) -> str:
    """A Jira comment as the GitHub-shaped body the assertions read.

    The hidden first line comes only from the ``curie.factory`` property.
    """

    marker = marker_of(properties)
    text = inert(adf_text(body))
    return f"<!-- {marker} -->\n{text}" if marker else text


def issue_of(state: Mapping[str, Any], issue_id: str) -> Mapping[str, Any]:
    for issue in state["issues"]:
        if issue["id"] == issue_id:
            return dict(issue)
    raise JiraStubError(f"the Jira stub has no issue {issue_id}")


def issue_record(issue: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "number": int(issue["id"]),
        "key": issue["key"],
        "labels": [{"name": label} for label in issue["labels"]],
    }


def issue_records(state: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [issue_record(issue) for issue in state["issues"]]


def comment_records(state: Mapping[str, Any], issue_id: str) -> list[dict[str, Any]]:
    issue = issue_of(state, issue_id)
    return [
        {
            "id": comment["id"],
            "issue_number": int(issue_id),
            "body": rendered(comment["body"], comment["properties"]),
            "user": {
                "login": comment["author"]["account_id"],
                "type": comment["author"]["account_type"],
            },
            "created_at": comment["created"],
        }
        for comment in issue["comments"]
    ]


def comment_history(state: Mapping[str, Any], comment_id: str) -> list[dict[str, Any]]:
    """The comment's bodies in the order the stub saw them, each with its instant."""

    return [
        {"at": entry["at"], "body": rendered(entry["body"], entry["properties"])}
        for entry in state["history"]
        if entry["comment_id"] == comment_id
    ]


def status_marker(request_id: str) -> str:
    """The hidden line a request's status comment record carries."""

    return f"<!-- curie-execution-request:{request_id} -->"


def line_instant(
    state: Mapping[str, Any], issue_id: str, request_id: str, line: str, bot: str
) -> Any:
    """When the request's own status comment first showed ``line``, or None."""

    marker = status_marker(request_id)
    for comment in comment_records(state, issue_id):
        if comment["user"]["login"] != bot:
            continue
        found = ots.first_line_instant(comment_history(state, comment["id"]), marker, line)
        if found is not None:
            return found
    return None


def final_comments(
    state: Mapping[str, Any],
    issue_id: str,
    request_id: str,
    notices: Sequence[Mapping[str, Any]],
    bot: str,
) -> list[dict[str, Any]]:
    """The request's status comment, once the platform recorded its final body.

    The platform finalizes a status comment when it writes the run's final body
    (``factory_terminal_notices.finalized_at``); the record names the comment.
    """

    finalized = {
        str(notice["comment_id"])
        for notice in notices
        if str(notice["execution_request_id"]) == request_id
        and notice.get("finalized_at") is not None
        and notice.get("comment_id") is not None
    }
    marker = status_marker(request_id)
    return [
        comment
        for comment in comment_records(state, issue_id)
        if comment["id"] in finalized
        and comment["user"]["login"] == bot
        and comment["body"].startswith(marker)
    ]


# Jira's own outcomes -------------------------------------------------------------


def browse_url(site_origin: str, key: str) -> str:
    return f"{site_origin.rstrip('/')}/browse/{key}"


def names_ticket(body: str | None, key: str, url: str) -> bool:
    """The body names the Jira key as a whole word and links its browse URL.

    Both are whole tokens: ``PROJ-10`` and its URL do not name ``PROJ-1``.
    """

    text = body or ""
    named = re.search(rf"(?<![A-Z0-9-]){re.escape(key)}(?![0-9])", text)
    linked = re.search(rf"{re.escape(url)}(?![0-9A-Za-z/_-])", text)
    return bool(named and linked)


def assert_one_jira_pull_request(
    pulls: Sequence[Mapping[str, Any]], key: str, url: str, before: Iterable[int]
) -> Mapping[str, Any]:
    """Session 2 opened exactly one new pull request, and it names this Jira ticket."""

    opened = ots.new_pulls(pulls, before)
    if len(opened) != 1:
        raise ots.OneTicketAssertionError(f"expected one new pull request, found {len(opened)}")
    if not names_ticket(opened[0].get("body"), key, url):
        raise ots.OneTicketAssertionError(
            f"the new pull request does not name {key} and its browse URL"
        )
    return opened[0]


def notices_sql(issue_id: str) -> str:
    """The status comment records of the Jira ticket's requests, read-only."""

    number = int(issue_id)
    return (
        "SELECT coalesce(json_agg(json_build_object("
        "'execution_request_id',n.execution_request_id,'comment_id',n.comment_id,"
        "'finalized_at',n.finalized_at)),'[]'::json) "
        "FROM curie.factory_terminal_notices n "
        "JOIN curie.execution_requests r ON r.id=n.execution_request_id "
        "JOIN curie.work_items w ON w.id=r.work_item_id "
        f"WHERE w.tracker_kind='jira_cloud' AND w.tracker_issue_id='{number}'"
    )


def cursor_sql(label: str) -> str:
    """The Jira site's stored poll cursor for ``label``, read-only, as JSON text.

    The tracker intake saves it only after every notice of a page was applied
    or deliberately refused (``factory_tracker_intake``), and its time is the
    newest issue update the page held.
    """

    return (
        "SELECT to_json(coalesce((SELECT etags->>'marked:' || "
        f"'{label}' FROM curie.factory_poll_cursors "
        f"WHERE tracker_kind='jira_cloud' AND tracker_scope_id='{CLOUD_ID}'),''))"
    )


def cursor_time_ms(cursor: Any) -> int | None:
    """The time of a Jira poll cursor ``<epoch ms>;<ids>``, or None."""

    if not isinstance(cursor, str):
        return None
    head, separator, _ids = cursor.partition(";")
    return int(head) if separator and head.isascii() and head.isdigit() else None


def instant_ms(value: Any) -> int:
    return int(ots.instant(value).timestamp() * 1000)


# Evidence --------------------------------------------------------------------

_STATUS_LINE = re.compile(r"^Status: ([A-Z][A-Z ]*)$", re.MULTILINE)
_CAUSE_LINE = re.compile(r"^Cause: ([a-z][a-z0-9_]*)$", re.MULTILINE)
_KNOWN_LINES = (ots.INVESTIGATING_LINE, ots.ANSWER_RECEIVED_LINE, "Could not complete:")


def comment_outcome(record: Mapping[str, Any], pull_url: str | None) -> dict[str, Any]:
    """A factory comment as fixed facts for public evidence, never its text.

    A status comment can quote run-specific URLs and worker-supplied detail,
    so only its marker kind, the platform's fixed lines it shows, its status
    and cause codes, and whether it names the pull request are kept.
    """

    body = str(record["body"])
    first = body.split("\n", 1)[0]
    if first.startswith(f"<!-- {ots.QUESTIONS_MARKER}"):
        marker = "questions"
    elif first.startswith("<!-- curie-execution-request:"):
        marker = "status"
    else:
        marker = None
    status = _STATUS_LINE.search(body)
    cause = _CAUSE_LINE.search(body)
    return {
        "id": str(record["id"]),
        "marker": marker,
        "lines": [line for line in _KNOWN_LINES if line in body],
        "status": status.group(1) if status else None,
        "cause": cause.group(1) if cause else None,
        "names_pull_request": bool(pull_url) and str(pull_url) in body,
    }
