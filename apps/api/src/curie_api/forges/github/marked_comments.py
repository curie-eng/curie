"""GitHub calls for marked status comments: find by marker, post, edit, reply."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

import httpx

from curie_api.factory_comment_text import _redact_factory_comment, marker_for
from curie_api.factory_reply_target import ReplyTarget
from curie_api.models import FactoryStatusComment, WorkItem

_REFUSED_STATUSES = {401, 403, 404}
_PAGES_PER_PASS = 5
_UNPROCESSABLE = ("unprocessable", "http_422")
# A thread target scans two lists with one stored page. Pages of the review
# comment list are stored as-is; once that list is exhausted, the conversation
# list page is stored above this offset.
_SECOND_LIST_OFFSET = 1_000_000


@dataclass(frozen=True)
class _MarkerScan:
    comment_id: int | None = None
    body: str | None = None
    refusal: str | None = None
    unavailable: bool = False
    next_page: int | None = None


@dataclass(frozen=True)
class _GitHub:
    client: httpx.AsyncClient
    api: str
    repo_path: str
    headers: dict[str, str]


async def _subject_title(github: _GitHub, work_item: WorkItem, target: ReplyTarget) -> str | None:
    """The issue or PR title for the card, read once. A failed read stays NULL."""

    if target.pr_number is None:
        path = f"{github.repo_path}/issues/{work_item.github_issue_number}"
    else:
        path = f"{github.repo_path}/pulls/{target.pr_number}"
    try:
        found = await github.client.get(
            f"{github.api}{path}", headers=github.headers, follow_redirects=False
        )
        payload = found.json() if found.status_code == 200 else None
    except (httpx.HTTPError, ValueError):
        return None
    if isinstance(payload, dict) and isinstance(payload.get("title"), str):
        return str(payload["title"])[:256]
    return None


CommentList = Literal["issue", "review"]


async def _deliver(
    github: _GitHub,
    work_item: WorkItem,
    row: FactoryStatusComment,
    target: ReplyTarget,
    body: str,
) -> tuple[Literal["refused"], str] | tuple[Literal["posted"], int, CommentList, bool] | None:
    """Create the comment, or find one a lost response already created.

    ``posted`` carries the comment id, the list it lives in, and whether this
    call created it with ``body``.
    """

    api, repo_path, headers, client = github.api, github.repo_path, github.headers, github.client
    number = work_item.github_issue_number if target.pr_number is None else target.pr_number
    comments_path = f"{repo_path}/issues/{number}/comments"
    marker = marker_for(row.execution_request_id)
    stored = max(1, row.scan_page)
    # (path, first page, offset stored for this list's next page, list)
    scans: list[tuple[str, int, int, CommentList]] = [(comments_path, stored, 0, "issue")]
    if target.kind == "thread":
        # A thread reply lands on the review comment list; its 422 fallback on
        # the conversation list. The marker may sit on either.
        review_path = f"{repo_path}/pulls/{number}/comments"
        if stored > _SECOND_LIST_OFFSET:
            scans = [(comments_path, stored - _SECOND_LIST_OFFSET, _SECOND_LIST_OFFSET, "issue")]
        else:
            scans = [
                (review_path, stored, 0, "review"),
                (comments_path, 1, _SECOND_LIST_OFFSET, "issue"),
            ]
    for path, start, offset, listed in scans:
        existing = await find_marker(client, api, path, headers, marker, start_page=start)
        if existing.refusal is not None:
            return ("refused", existing.refusal)
        if existing.unavailable:
            return None
        if existing.comment_id is not None:
            return ("posted", existing.comment_id, listed, False)
        if existing.next_page is not None:
            row.scan_page = existing.next_page + offset
            return None
    if target.kind == "thread":
        assert target.comment_id is not None
        root = await _thread_root(client, api, repo_path, headers, target.comment_id)
        replied = await _post(
            client,
            f"{api}{repo_path}/pulls/{number}/comments/{root}/replies",
            headers,
            body,
        )
        if replied != _UNPROCESSABLE:
            return _created(row, replied, "review")
    posted = await _post(client, f"{api}{comments_path}", headers, body)
    # A comment GitHub cannot process stays pending, as before #2798.
    if posted == _UNPROCESSABLE:
        return None
    return _created(row, posted, "issue")


def _created(
    row: FactoryStatusComment, outcome: tuple[str, str] | None, listed: CommentList
) -> tuple[Literal["refused"], str] | tuple[Literal["posted"], int, CommentList, bool] | None:
    if outcome is None:
        # Lost response after a possible success: rescan every list next pass.
        row.scan_page = 1
        return None
    if outcome[0] == "refused":
        return ("refused", outcome[1])
    return ("posted", int(outcome[1]), listed, True)


async def _patch(
    github: _GitHub, row: FactoryStatusComment, body: str
) -> Literal["edited", "gone"] | str | None:
    """Edit the comment in place: ``edited``, ``gone`` (404), a refusal, or None."""

    kind = "pulls" if row.comment_list == "review" else "issues"
    url = f"{github.api}{github.repo_path}/{kind}/comments/{row.comment_id}"
    try:
        edited = await github.client.patch(
            url,
            headers=github.headers,
            json={"body": _redact_factory_comment(body)},
            follow_redirects=False,
        )
    except httpx.HTTPError:
        return None
    if edited.status_code == 200:
        return "edited"
    if edited.status_code == 404:
        return "gone"
    if edited.status_code in {401, 403}:
        return f"http_{edited.status_code}"
    return None


async def _thread_root(
    client: httpx.AsyncClient,
    api: str,
    repo_path: str,
    headers: dict[str, str],
    comment_id: int,
) -> int:
    """Reply to the thread's first comment; replies to replies are refused."""

    try:
        found = await client.get(
            f"{api}{repo_path}/pulls/comments/{comment_id}",
            headers=headers,
            follow_redirects=False,
        )
        payload = found.json() if found.status_code == 200 else None
    except (httpx.HTTPError, ValueError):
        payload = None
    if isinstance(payload, dict):
        root = payload.get("in_reply_to_id")
        if type(root) is int and root > 0:
            return root
    return comment_id


async def _post(
    client: httpx.AsyncClient, url: str, headers: dict[str, str], body: str
) -> tuple[str, str] | None:
    try:
        created = await client.post(
            url,
            headers=headers,
            json={"body": _redact_factory_comment(body)},
            follow_redirects=False,
        )
    except httpx.HTTPError:
        return None
    if created.status_code == 422:
        return _UNPROCESSABLE
    if created.status_code in _REFUSED_STATUSES:
        return ("refused", f"http_{created.status_code}")
    if created.status_code not in {200, 201}:
        return None
    try:
        payload = created.json()
    except ValueError:
        return None
    if not isinstance(payload, dict) or type(payload.get("id")) is not int:
        return None
    return ("posted", str(payload["id"]))


async def find_marker(
    client: httpx.AsyncClient,
    api: str,
    path: str,
    headers: dict[str, str],
    marker: str,
    *,
    start_page: int,
    app_id: str | None = None,
) -> _MarkerScan:
    """Find a marker, or remember the next page so a later pass can continue.

    A full page does not mean the marker is absent. Stopping there and posting
    would duplicate a comment that sits further down the list. A short page is
    the end, so a missing marker is safe to post.
    """

    page = start_page
    for _ in range(_PAGES_PER_PASS):
        try:
            listed = await client.get(
                f"{api}{path}",
                headers=headers,
                params={"per_page": 100, "page": page},
                follow_redirects=False,
            )
        except httpx.HTTPError:
            return _MarkerScan(unavailable=True)
        if listed.status_code in _REFUSED_STATUSES:
            return _MarkerScan(refusal=f"http_{listed.status_code}")
        if listed.status_code != 200:
            return _MarkerScan(unavailable=True)
        try:
            payload = listed.json()
        except ValueError:
            return _MarkerScan(unavailable=True)
        found = _marked_comment(payload, marker, app_id=app_id)
        if found is not None:
            return _MarkerScan(comment_id=found[0], body=found[1])
        if not isinstance(payload, list) or len(payload) < 100:
            return _MarkerScan()
        page += 1
    return _MarkerScan(next_page=page)


def _marked_comment(
    payload: Any, marker: str, *, app_id: str | None = None
) -> tuple[int, str] | None:
    """The first comment carrying ``marker``.

    ``app_id`` None reads every author. Otherwise only comments the app with
    that id posted count, and an empty id matches none.
    """

    if not isinstance(payload, list):
        return None
    for item in payload:
        if not isinstance(item, dict):
            continue
        if app_id is not None:
            via = item.get("performed_via_github_app")
            if not app_id or not isinstance(via, dict) or str(via.get("id")) != app_id:
                continue
        body = item.get("body")
        comment_id = item.get("id")
        if isinstance(body, str) and marker in body and type(comment_id) is int:
            return comment_id, body
    return None


async def upsert_issue_notice(
    client: httpx.AsyncClient,
    *,
    api: str,
    repo_path: str,
    headers: dict[str, str],
    issue_number: int,
    marker: str,
    body: str,
    app_id: str = "",
) -> Literal["written", "unchanged", "unavailable"]:
    """Keep one marked issue comment carrying ``body``: post it, edit it, or leave it.

    The marker scan is the same one the status comment uses. A scan that could
    not reach the end of the list is ``unavailable``, never a second post.
    """

    body = _redact_factory_comment(body)
    comments_path = f"{repo_path}/issues/{issue_number}/comments"
    found = await find_marker(
        client,
        api,
        comments_path,
        headers,
        marker,
        start_page=1,
        app_id=app_id.strip(),
    )
    if found.unavailable or found.refusal is not None or found.next_page is not None:
        return "unavailable"
    if found.comment_id is not None:
        if found.body == body:
            return "unchanged"
        try:
            edited = await client.patch(
                f"{api}{repo_path}/issues/comments/{found.comment_id}",
                headers=headers,
                json={"body": body},
                follow_redirects=False,
            )
        except httpx.HTTPError:
            return "unavailable"
        return "written" if edited.status_code == 200 else "unavailable"
    posted = await _post(client, f"{api}{comments_path}", headers, body)
    if posted is None or posted[0] != "posted":
        return "unavailable"
    return "written"
