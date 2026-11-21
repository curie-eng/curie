"""The GitHub issue read keeps the newest comments of a long thread (#5787).

A thread longer than ten pages of comments reads the newest thousand, oldest
first (the ten pages ending at its last page, plus one earlier page when the
last is partial), and says the earlier comments were not read. The
`Link` header shape is
https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api:
`rel="next"` and `rel="last"` on every page but the last, `rel="prev"` and
`rel="first"` on every page but the first. The tracker is driven through
`read_ticket`, the renderer the issue-read route serves.
"""

from __future__ import annotations

import asyncio
import math

import httpx
import pytest
from curie_api.forges.errors import Unavailable
from curie_api.forges.github.comments import static_token
from curie_api.forges.github.tracker import GitHubTracker

REPO = "acme/widgets"
REPO_ID = 9001
NUMBER = 7
PER_PAGE = 100
API = "https://api.github.com"
ISSUE_PATH = f"/repositories/{REPO_ID}/issues/{NUMBER}"


def _comment(index: int) -> dict[str, object]:
    return {
        "user": {"login": "maintainer"},
        "created_at": f"2026-10-01T00:{index // 60 % 60:02d}:{index % 60:02d}Z",
        "body": f"comment {index}",
    }


def _link(url: httpx.URL, page: int, last: int, *, with_last: bool = True) -> str:
    def to(target: int, rel: str) -> str:
        return f'<{url.copy_set_param("page", str(target))}>; rel="{rel}"'

    parts = []
    if page > 1:
        parts.append(to(page - 1, "prev"))
    if page < last:
        parts.append(to(page + 1, "next"))
        if with_last:
            parts.append(to(last, "last"))
    if page > 1:
        parts.append(to(1, "first"))
    return ", ".join(parts)


def _read(total: int, *, with_last: bool = True) -> tuple[str, list[int]]:
    requested: list[int] = []

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == ISSUE_PATH:
            return httpx.Response(
                200,
                json={
                    "number": NUMBER,
                    "title": "Long thread",
                    "body": "The description.",
                    "state": "open",
                    "user": {"login": "reporter"},
                },
            )
        assert request.url.path == f"{ISSUE_PATH}/comments"
        assert request.url.params["per_page"] == str(PER_PAGE)
        page = int(request.url.params["page"])
        requested.append(page)
        last = max(1, math.ceil(total / PER_PAGE))
        batch = [_comment(i) for i in range(1, total + 1)][(page - 1) * PER_PAGE : page * PER_PAGE]
        headers = {"link": _link(request.url, page, last, with_last=with_last)} if last > 1 else {}
        return httpx.Response(200, json=batch, headers=headers)

    async def own_login() -> str:
        return "curie[bot]"

    async def run() -> str:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            tracker = GitHubTracker(
                client,
                api=API,
                html_base="https://github.com",
                repo_full_name=REPO,
                repository_id=REPO_ID,
                token=static_token("fixture-installation-token"),
                label="curie",
                mention="@curie",
                app_id="51",
                own_login=own_login,
            )
            ticket = await tracker.read_ticket(tracker.issue(NUMBER))
            return ticket.ticket + "\n" + ticket.thread_context

    return asyncio.run(run()), requested


def test_a_long_thread_renders_its_newest_thousand_in_order_after_the_marker() -> None:
    ticket, requested = _read(1300)

    # Page 1 sizes the thread; the ten pages ending at the last page are read.
    assert requested == [1, *range(4, 14)]
    sections = ticket.split("\n\n## Comment by ")
    assert sections[0] == "# Long thread\n\nThe description.\n\n_Earlier comments were not read._"
    bodies = [section.split("\n\n", 1)[1].strip() for section in sections[1:]]
    assert bodies == [f"comment {i}" for i in range(301, 1301)]
    assert "Later comments" not in ticket


def test_a_partial_last_page_still_ends_at_the_newest_comment() -> None:
    ticket, requested = _read(1250)

    # The partial last page adds page 3 so the window holds a full thousand.
    assert sorted(requested) == [1, *range(3, 14)]
    sections = ticket.split("\n\n## Comment by ")
    assert sections[0].endswith("_Earlier comments were not read._")
    bodies = [section.split("\n\n", 1)[1].strip() for section in sections[1:]]
    assert bodies == [f"comment {i}" for i in range(251, 1251)]


def test_a_thousand_comments_are_read_whole_without_a_marker() -> None:
    ticket, requested = _read(1000)

    assert requested == list(range(1, 11))
    assert "not read" not in ticket
    assert ticket.count("## Comment by ") == 1000


def test_a_short_thread_renders_exactly_as_before() -> None:
    ticket, requested = _read(3)

    assert requested == [1]
    assert ticket == (
        "# Long thread\n\nThe description.\n\n"
        "## Comment by maintainer at 2026-10-01T00:00:01Z\n\ncomment 1\n\n"
        "## Comment by maintainer at 2026-10-01T00:00:02Z\n\ncomment 2\n\n"
        "## Comment by maintainer at 2026-10-01T00:00:03Z\n\ncomment 3\n"
    )


def test_a_next_only_listing_is_followed_forward_and_returned_whole() -> None:
    ticket, requested = _read(250, with_last=False)

    assert requested == [1, 2, 3]
    assert "not read" not in ticket
    bodies = [s.split("\n\n", 1)[1].strip() for s in ticket.split("\n\n## Comment by ")[1:]]
    assert bodies == [f"comment {i}" for i in range(1, 251)]


def test_a_next_only_listing_beyond_ten_pages_is_unavailable() -> None:
    with pytest.raises(Unavailable):
        _read(1100, with_last=False)
